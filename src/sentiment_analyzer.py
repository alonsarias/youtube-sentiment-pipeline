from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from config import SentimentConfig, logger

class SentimentAnalyzer:
    """
    Performs sentiment analysis using a pre-trained transformer model.

    This class encapsulates the loading and inference functionality for sentiment
    analysis using HuggingFace's Transformers library.
    """

    def __init__(self, model_name=None):
        """
        Initialize the sentiment analyzer with a pre-trained model.

        Args:
            model_name (str, optional): HuggingFace model identifier.
                Defaults to value from SentimentConfig.
        """
        self.model_name = model_name or SentimentConfig.MODEL_NAME
        logger.info(f"Loading sentiment analysis model: {self.model_name}")

        # Load pre-trained tokenizer and model from HuggingFace
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.sentiment_map = SentimentConfig.SENTIMENT_MAP
        logger.info("Sentiment analyzer initialized successfully")

    def predict_sentiment(self, texts):
        """
        Predict sentiment for text input(s).

        Args:
            texts (str or list): Single text or list of texts to analyze

        Returns:
            list: List of sentiment labels (e.g., "Positive", "Negative", "Neutral")
        """
        # Handle single text input
        if isinstance(texts, str):
            texts = [texts]

        # Tokenize inputs with padding and truncation
        inputs = self.tokenizer(
            texts,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=SentimentConfig.MAX_LENGTH
        )

        # Perform inference without computing gradients
        with torch.no_grad():
            outputs = self.model(**inputs)

        # Calculate probabilities and get predicted classes
        probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
        predicted_classes = torch.argmax(probabilities, dim=-1).tolist()

        # Map numerical classes to sentiment labels
        sentiments = [self.sentiment_map[cls] for cls in predicted_classes]

        return sentiments

    def predict_sentiment_with_scores(self, texts):
        """
        Predict sentiment with confidence scores for text input(s).

        Args:
            texts (str or list): Single text or list of texts to analyze

        Returns:
            list: List of tuples (sentiment_label, confidence_score)
                where confidence_score is a float between 0 and 1
        """
        # Handle single text input
        if isinstance(texts, str):
            texts = [texts]

        # Tokenize inputs with padding and truncation
        inputs = self.tokenizer(
            texts,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=SentimentConfig.MAX_LENGTH
        )

        # Perform inference without computing gradients
        with torch.no_grad():
            outputs = self.model(**inputs)

        # Calculate probabilities and extract predicted classes with their confidence scores
        probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
        predicted_classes = torch.argmax(probabilities, dim=-1).tolist()
        confidence_scores = torch.max(probabilities, dim=-1)[0].tolist()

        # Combine sentiment labels with their confidence scores
        results = [
            (self.sentiment_map[cls], score)
            for cls, score in zip(predicted_classes, confidence_scores)
        ]

        return results