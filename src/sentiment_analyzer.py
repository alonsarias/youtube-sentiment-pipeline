from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

class SentimentAnalyzer:
    """Class to perform sentiment analysis using a pre-trained model."""

    def __init__(self, model_name="tabularisai/multilingual-sentiment-analysis"):
        """Initialize the sentiment analyzer with a pre-trained model.

        Args:
            model_name (str): HuggingFace model name/path
        """
        print(f"Loading model: {model_name}")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.sentiment_map = {
            0: "Very Negative",
            1: "Negative",
            2: "Neutral",
            3: "Positive",
            4: "Very Positive"
        }
        print("Sentiment analyzer initialized successfully")

    def predict_sentiment(self, texts):
        """Predict sentiment for a list of texts.

        Args:
            texts (list or str): Text(s) to analyze

        Returns:
            list: List of sentiment labels
        """
        # Handle single text input
        if isinstance(texts, str):
            texts = [texts]

        # Tokenize inputs
        inputs = self.tokenizer(
            texts,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=512
        )

        # Perform inference
        with torch.no_grad():
            outputs = self.model(**inputs)

        # Get probabilities and predicted classes
        probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
        predicted_classes = torch.argmax(probabilities, dim=-1).tolist()

        # Map numerical classes to sentiment labels
        sentiments = [self.sentiment_map[cls] for cls in predicted_classes]

        return sentiments

    def predict_sentiment_with_scores(self, texts):
        """Predict sentiment with confidence scores for a list of texts.

        Args:
            texts (list or str): Text(s) to analyze

        Returns:
            list: List of tuples (sentiment_label, confidence_score)
        """
        # Handle single text input
        if isinstance(texts, str):
            texts = [texts]

        # Tokenize inputs
        inputs = self.tokenizer(
            texts,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=512
        )

        # Perform inference
        with torch.no_grad():
            outputs = self.model(**inputs)

        # Get probabilities and predicted classes
        probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
        predicted_classes = torch.argmax(probabilities, dim=-1).tolist()
        confidence_scores = torch.max(probabilities, dim=-1)[0].tolist()

        # Map numerical classes to sentiment labels with confidence scores
        results = [
            (self.sentiment_map[cls], score)
            for cls, score in zip(predicted_classes, confidence_scores)
        ]

        return results