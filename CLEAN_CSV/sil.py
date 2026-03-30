import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"

import pandas as pd
import numpy as np
import tensorflow as tf
import keras_nlp
from sklearn.model_selection import train_test_split
from sklearn.metrics import log_loss
import warnings

warnings.filterwarnings("ignore")

print("TensorFlow version:", tf.__version__)
print("KerasNLP version:", keras_nlp.__version__)

# ============================================
# 1. LOAD DATA
# ============================================
print("\n" + "=" * 50)
print("Loading data...")
print("=" * 50)

train = pd.read_csv(
    "/kaggle/input/competitions/llm-classification-finetuning/train.csv"
)
test = pd.read_csv("/kaggle/input/competitions/llm-classification-finetuning/test.csv")

print(f"Train shape: {train.shape}")
print(f"Test shape: {test.shape}")

# ============================================
# 2. CLEAN AND PREPARE TEXT
# ============================================
print("\n" + "=" * 50)
print("Cleaning text data...")
print("=" * 50)


def clean_text(text):
    """Remove list brackets and quotes from text"""
    text = str(text)
    # Remove brackets if present
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    # Remove quotes
    text = text.strip("\"'")
    # Replace escaped quotes
    text = text.replace('\\"', '"')
    return text


# Clean all text columns
for col in ["prompt", "response_a", "response_b"]:
    train[col] = train[col].apply(clean_text)
    test[col] = test[col].apply(clean_text)


# Create formatted text input
def format_text(row):
    return f"""Prompt: {row["prompt"]}

Response A: {row["response_a"]}

Response B: {row["response_b"]}"""


train["text"] = train.apply(format_text, axis=1)
test["text"] = test.apply(format_text, axis=1)

print("Sample text:")
print(train["text"].iloc[0][:300] + "...")

# ============================================
# 3. CREATE TARGETS
# ============================================
print("\n" + "=" * 50)
print("Creating targets...")
print("=" * 50)

train["label"] = train.apply(
    lambda x: 0 if x["winner_model_a"] == 1 else (1 if x["winner_model_b"] == 1 else 2),
    axis=1,
)

print("Class distribution:")
print(train["label"].value_counts(normalize=True).sort_index())
print("\nCounts:")
print(train["label"].value_counts().sort_index())

# ============================================
# 4. SPLIT DATA
# ============================================
print("\n" + "=" * 50)
print("Splitting data...")
print("=" * 50)

train_texts, val_texts, train_labels, val_labels = train_test_split(
    train["text"].values,
    train["label"].values,
    test_size=0.1,  # Smaller validation set for faster training
    random_state=42,
    stratify=train["label"].values,
)

print(f"Train samples: {len(train_texts)}")
print(f"Validation samples: {len(val_texts)}")

# ============================================
# 5. SIMPLE MODEL USING BUILT-IN CLASSIFIER
# ============================================
print("\n" + "=" * 50)
print("Building model...")
print("=" * 50)

# Use the built-in classifier which handles everything
# This is the simplest approach that should work without errors
model = keras_nlp.models.DebertaV3Classifier.from_preset(
    "deberta_v3_base_en",
    num_classes=3,
)

# Compile the model
model.compile(
    optimizer=tf.keras.optimizers.AdamW(learning_rate=2e-5),
    loss="sparse_categorical_crossentropy",
    metrics=["accuracy"],
)

print("Model summary:")
model.summary()

# ============================================
# 6. TRAIN THE MODEL
# ============================================
print("\n" + "=" * 50)
print("Training model...")
print("=" * 50)

# Simple training without complex callbacks
history = model.fit(
    train_texts,
    train_labels,
    validation_data=(val_texts, val_labels),
    batch_size=4,  # Small batch size for GPU memory
    epochs=3,
    verbose=1,
)

# ============================================
# 7. EVALUATE ON VALIDATION
# ============================================
print("\n" + "=" * 50)
print("Evaluating on validation...")
print("=" * 50)

# Get predictions
val_preds = model.predict(val_texts)

# Calculate log loss
val_loss = log_loss(val_labels, val_preds)
print(f"Validation Log Loss: {val_loss:.4f}")

# Calculate accuracy
val_acc = (val_preds.argmax(axis=1) == val_labels).mean()
print(f"Validation Accuracy: {val_acc:.4f}")

# ============================================
# 8. CREATE SUBMISSION
# ============================================
print("\n" + "=" * 50)
print("Creating submission...")
print("=" * 50)

# Predict on test
test_preds = model.predict(test["text"].values)

# Create submission
submission = pd.DataFrame(
    {
        "id": test["id"],
        "winner_model_a": test_preds[:, 0],
        "winner_model_b": test_preds[:, 1],
        "winner_tie": test_preds[:, 2],
    }
)

# Save submission
submission.to_csv("submission.csv", index=False)
print("Submission saved!")
print("\nSubmission preview:")
print(submission.head())

# ============================================
# 9. IMPROVEMENT OPTIONS
# ============================================
print("\n" + "=" * 50)
print("Improvements you can try:")
print("=" * 50)
print("""
1. **Model Size**:
   - Use "deberta_v3_small_en" for faster training
   - Use "deberta_v3_large_en" for better accuracy (requires more memory)

2. **Training**:
   - Increase epochs to 5-10
   - Try different learning rates (1e-5, 3e-5, 5e-5)
   - Use learning rate scheduling

3. **Data**:
   - Add feature engineering (length differences, etc.)
   - Try different prompt formatting
   - Use data augmentation

4. **Cross-validation**:
   - Implement k-fold cross-validation
   - Ensemble multiple models

5. **Advanced**:
   - Use LoRA for efficient fine-tuning
   - Try different model architectures
   - Add model name embeddings
""")

print("\n" + "=" * 50)
print("Done! Ready to submit to competition.")
print("=" * 50)
