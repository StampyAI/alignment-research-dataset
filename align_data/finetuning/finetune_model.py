import torch
import torch.nn as nn
import torch.optim as optim
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.utils.data import DataLoader
from openai.embeddings_utils import get_embeddings

from finetuning_dataset import FinetuningDataset


class ContrastiveLoss(nn.Module):
    def __init__(self, margin=2.0):
        super(ContrastiveLoss, self).__init__()
        self.margin = margin

    def forward(self, output1, output2, label):
        euclidean_distance = nn.functional.pairwise_distance(output1, output2)
        loss_contrastive = torch.mean(
            (1-label) * torch.pow(euclidean_distance, 2)
            + (label) * torch.pow(torch.clamp(self.margin - euclidean_distance, min=0.0), 2)
        )

        return loss_contrastive


class FineTuneModel(nn.Module):
    def __init__(self, embedding_dim, hidden_dim, dropout=0.5):
        super(FineTuneModel, self).__init__()
        
        self.fc1 = nn.Linear(embedding_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, embedding_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        x = nn.functional.relu(self.fc1(x))
        x = self.dropout(x)
        x = self.fc2(x)
        return x


def train(model, dataloader, optimizer, criterion, device):
    model.train()
    total_loss = 0.0
    
    for batch_idx, (text1_embedding, text2_embedding, target) in enumerate(dataloader):
        text1_embedding = text1_embedding.to(device)
        text2_embedding = text2_embedding.to(device)
        target = target.float().to(device)

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


def validate(model, dataloader, criterion, device):
    model.eval()
    total_loss = 0.0

    with torch.no_grad():
        for batch_idx, (text1_embedding, text2_embedding, target) in enumerate(dataloader):
            target = target.float().to(device)
            
            output1 = model(text1_embedding)
            output2 = model(text2_embedding)

            loss = criterion(output1, output2, target)
            total_loss += loss.item()

    return total_loss / len(dataloader)


def finetune_embeddings():
    # Hyperparameters & Configuration
    EMBEDDING_DIM = 1536
    HIDDEN_DIM = 512
    EPOCHS = 10
    LEARNING_RATE = 0.001
    MARGIN = 2.0
    DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    dataset = FinetuningDataset()
    dataloader = DataLoader(dataset, batch_size=32, num_workers=4)  # Increase num_workers

    model = FineTuneModel(EMBEDDING_DIM, HIDDEN_DIM).to(DEVICE)
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
    scheduler = ReduceLROnPlateau(optimizer, 'min', patience=2, factor=0.5, verbose=True)
    criterion = ContrastiveLoss(MARGIN)

    # Assuming you've split your data and have a separate validation set
    validation_dataset = FinetuningDataset()
    validation_dataloader = DataLoader(validation_dataset, batch_size=32, num_workers=4)
    best_val_loss = float('inf')
    
    epochs_without_improvement = 0
    max_epochs_without_improvement = 5  # stop after 5 epochs without improvement
    
    for epoch in range(EPOCHS):
        train_loss = train(model, dataloader, optimizer, criterion, DEVICE)
        validate_loss = validate(model, validation_dataloader, criterion, DEVICE)
        
        scheduler.step(validate_loss)
        if validate_loss < best_val_loss:
            best_val_loss = validate_loss
            torch.save(model.state_dict(), 'best_finetuned_model.pth')
            epochs_without_improvement = 0
        else:
            epochs_without_improvement += 1

        print(f'Epoch: {epoch+1}/{EPOCHS} | Train Loss: {train_loss:.4f} | Validation Loss: {validate_loss:.4f}')

        if epochs_without_improvement >= max_epochs_without_improvement:
            print("Early stopping due to no improvement in validation loss.")
            break
    
    # Save model
    torch.save(model.state_dict(), 'finetuned_model.pth')
