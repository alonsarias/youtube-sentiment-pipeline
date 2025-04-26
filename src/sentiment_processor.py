from sentiment_analyzer import SentimentAnalyzer
from hbase_utils import update_comment_with_sentiment, get_comment
from config import logger

class SentimentProcessor:
    """Process comments and perform sentiment analysis."""

    def __init__(self):
        """Initialize the sentiment processor with a sentiment analyzer."""
        self.analyzer = SentimentAnalyzer()
        logger.info("Sentiment processor initialized")

    def process_comment(self, connection, row_key, comment_text=None):
        """Process a comment by performing sentiment analysis and updating HBase.

        Args:
            connection: HBase connection
            row_key: Unique identifier for the row
            comment_text: Optional comment text. If None, will fetch from HBase.

        Returns:
            str: The sentiment label
        """
        # If comment text is not provided, fetch it from HBase
        if comment_text is None:
            comment_data = get_comment(connection, row_key)
            comment_text = comment_data.get('comment')

        if not comment_text:
            logger.warning(f"Warning: No comment text found for row_key: {row_key}")
            return None

        # Perform sentiment analysis
        sentiment = self.analyzer.predict_sentiment(comment_text)[0]
        logger.debug(f"Analyzed sentiment for row_key {row_key}: {sentiment}")

        # Update HBase with sentiment
        update_comment_with_sentiment(connection, row_key, sentiment)

        return sentiment