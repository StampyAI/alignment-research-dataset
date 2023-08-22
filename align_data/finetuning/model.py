import os

import torch
import torch.nn as nn
import torch.optim as optim
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.utils.data import DataLoader

from align_data.finetuning.dataset import FinetuningDataset
from align_data.settings import (
    PINECONE_VALUES_DIMS,
    DEVICE,
    OPENAI_FINETUNED_LAYER_PATH,
    OPENAI_CURRENT_BEST_FINETUNED_LAYER_PATH,
)


class ContrastiveLoss(nn.Module):
    def __init__(self, margin=2.0):
        super(ContrastiveLoss, self).__init__()
        self.margin = margin

    def forward(self, output1, output2, label):
        euclidean_distance = nn.functional.pairwise_distance(output1, output2)
        loss_contrastive = torch.mean(
            (1 - label) * torch.pow(euclidean_distance, 2)
            + (label) * torch.pow(torch.clamp(self.margin - euclidean_distance, min=0.0), 2)
        )

        return loss_contrastive


class NonLinearFineTuneModel(nn.Module):
    def __init__(self, embedding_dim=PINECONE_VALUES_DIMS, hidden_dim=2000, dropout=0.5):
        super(FineTuneModel, self).__init__()

        self.fc1 = nn.Linear(embedding_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, embedding_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        x = nn.functional.relu(self.fc1(x))
        x = self.dropout(x)
        x = self.fc2(x)
        return x


class FineTuneModel(nn.Module):
    def __init__(self, embedding_dim=PINECONE_VALUES_DIMS):
        super(FineTuneModel, self).__init__()

        self.fc = nn.Linear(embedding_dim, embedding_dim)

    def forward(self, x):
        x = self.fc(x)
        return x


def train(model, dataloader, optimizer, criterion):
    model.train()
    total_loss = 0.0

    for batch_idx, (text1_embedding, text2_embedding, target) in enumerate(dataloader):
        text1_embedding = text1_embedding.to(DEVICE)
        text2_embedding = text2_embedding.to(DEVICE)
        target = target.float().to(DEVICE)

        optimizer.zero_grad()

        output1 = model(text1_embedding)
        output2 = model(text2_embedding)

        loss = criterion(output1, output2, target)
        loss.backward()

        # Gradient clipping
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)

        optimizer.step()

        total_loss += loss.item()

    return total_loss / len(dataloader)


def validate(model, dataloader, criterion):
    model.eval()
    total_loss = 0.0

    with torch.no_grad():
        for batch_idx, (text1_embedding, text2_embedding, target) in enumerate(dataloader):
            text1_embedding = text1_embedding.to(DEVICE)
            text2_embedding = text2_embedding.to(DEVICE)
            target = target.float().to(DEVICE)

            output1 = model(text1_embedding)
            output2 = model(text2_embedding)

            loss = criterion(output1, output2, target)
            total_loss += loss.item()

    return total_loss / len(dataloader)


def finetune_embeddings():
    # Hyperparameters & Configuration
    EPOCHS = 100
    BATCH_PER_EPOCH = 20
    BATCH_SIZE = 64
    LEARNING_RATE = 5.0000e-02
    MARGIN = 2.0

    dataset = FinetuningDataset(num_batches_per_epoch=BATCH_PER_EPOCH)
    dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, num_workers=5)

    model = FineTuneModel().to(DEVICE)
    model = load_best_model_if_exists(model)
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
    scheduler = ReduceLROnPlateau(optimizer, "min", patience=2, factor=0.5, verbose=True)
    criterion = ContrastiveLoss(MARGIN)

    # Assuming you've split your data and have a separate validation set
    validation_dataset = FinetuningDataset(num_batches_per_epoch=BATCH_PER_EPOCH)
    validation_dataloader = DataLoader(validation_dataset, batch_size=BATCH_SIZE, num_workers=5)
    best_val_loss = validate(model, validation_dataloader, criterion)
    print(f"Initial validation loss (from loaded model or new model): {best_val_loss:.4f}")

    epochs_without_improvement = 0
    max_epochs_without_improvement = 15  # stop after 5 epochs without improvement

    for epoch in range(EPOCHS):
        train_loss = train(model, dataloader, optimizer, criterion)
        validate_loss = validate(model, validation_dataloader, criterion)

        scheduler.step(validate_loss)
        if validate_loss < best_val_loss:
            best_val_loss = validate_loss
            torch.save(model.state_dict(), OPENAI_CURRENT_BEST_FINETUNED_LAYER_PATH)
            epochs_without_improvement = 0
        else:
            epochs_without_improvement += 1

        print(
            f"Epoch: {epoch+1}/{EPOCHS} | Train Loss: {train_loss:.4f} | Validation Loss: {validate_loss:.4f}"
        )

        if epochs_without_improvement >= max_epochs_without_improvement:
            print("Early stopping due to no improvement in validation loss.")
            break

    torch.save(model.state_dict(), OPENAI_FINETUNED_LAYER_PATH)


### HELPER FUNCTIONS ###


def load_best_model_if_exists(model):
    """
    Load the best saved model if it exists.

    Parameters:
    - model (torch.nn.Module): The model architecture.

    Returns:
    - model (torch.nn.Module): The loaded model.
    """
    if os.path.exists(OPENAI_CURRENT_BEST_FINETUNED_LAYER_PATH):
        model.load_state_dict(
            torch.load(OPENAI_CURRENT_BEST_FINETUNED_LAYER_PATH, map_location=DEVICE)
        )
        print(f"Loaded model from {OPENAI_CURRENT_BEST_FINETUNED_LAYER_PATH}.")
    else:
        print(
            f"No saved model found at {OPENAI_CURRENT_BEST_FINETUNED_LAYER_PATH}. Starting from scratch."
        )

    return model
