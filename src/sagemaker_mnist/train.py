from __future__ import annotations

import argparse
import json
import os

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

class LeNet5(nn.Module):
    def __init__(self):
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(1, 6, kernel_size = 5),
            nn.Tanh(),
            nn.AvgPool2d(kernel_size=2),
            nn.Conv2d(6, 16, kernel_size=5),
            nn.Tanh(),
            nn.AvgPool2d(kernel_size=2),
        )
        self.classifier = nn.Sequential(
            nn.Flatten(),
            nn.Linear(16 * 4 * 4, 120),
            nn.Tanh(),
            nn.Linear(120, 84),
            nn.Tanh(),
            nn.Linear(84, 10),
        )
    
    def forward(self, x):
        x = self.features(x)
        return self.classifier(x)

def train(args):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    train_bundle = torch.load(os.path.join(args.train, "train.pt"), map_location="cpu")
    test_bundle = torch.load(os.path.join(args.test, "test.pt"), map_location="cpu")

    x_train, y_train = train_bundle["images"], train_bundle["labels"]
    x_test, y_test = test_bundle["images"], test_bundle["labels"]

    train_loader = DataLoader(TensorDataset(x_train, y_train), batch_size = args.batch_size, shuffle = True)
    test_loader = DataLoader(TensorDataset(x_test, y_test), batch_size = args.batch_size, shuffle = False)

    model = LeNet5().to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=args.lr)

    for epoch in range(args.epochs):
        model.train()
        running_loss = 0.0

        for images, labels in train_loader:
            images, labels = images.to(device), labels.to(device)

            optimizer.zero_grad()
            logits = model(images)
            loss = criterion(logits, labels)
            loss.backward()
            optimizer.step()

            running_loss += loss.item()

        print(f"epoch={epoch + 1}, loss ={running_loss / len(train_loader): .4f}")

    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for images, labels in test_loader:
            images, labels = images.to(device), labels.to(device)
            logits = model(images)
            pred = torch.argmax(logits, dim = 1)
            correct += (pred == labels).sum().item()
            total += labels.size(0)

        accuracy = correct / total
        print(f"test_accuracy={accuracy:.4f}")

        model_dir = os.environ["SM_MODEL_DIR"]
        os.makedirs(model_dir, exist_ok=True)

        torch.save(model.state_dict(), os.path.join(model_dir, "model.pth"))

        model_cpu = model.cpu()
        scripted = torch.jit.script(model_cpu)
        scripted.save(os.path.join(model_dir, "model.ts"))

        with open(os.path.join(model_dir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump(
                {
                    "model_type": "LeNet5",
                    "num_classes": 10,
                    "input_shape": [1, 28, 28],
                    "test_accuracy": accuracy,
                },
                f,
                ensure_ascii = False,
                indent = 2,
            )

if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        parser.add_argument("--epochs", type=int, default = 3)
        parser.add_argument("--batch-size", type=int, default = 128)
        parser.add_argument("--lr", type=float, default = 0.001)
        parser.add_argument("--train", type=str, default = os.environ.get("SM_CHANNEL_TRAIN"))
        parser.add_argument("--test", type=str, default = os.environ.get("SM_CHANNEL_TEST"))
        args = parser.parse_args()
        train(args)

