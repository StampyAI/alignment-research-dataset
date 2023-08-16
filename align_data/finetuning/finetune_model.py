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
