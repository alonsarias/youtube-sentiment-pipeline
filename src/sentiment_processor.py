from sentiment_analyzer import SentimentAnalyzer
from hbase_utils import update_comment_with_sentiment, get_comment
from config import logger

class SentimentProcessor:
    """
    Orchestrates sentiment analysis workflow and storage integration.

    This class acts as a bridge between the sentiment analyzer and data storage layers,
    handling the complete flow of comment processing from analysis to persistence.
    """

    def __init__(self):
        """Initialize the sentiment processor with required components."""
        self.analyzer = SentimentAnalyzer()
        logger.info("Sentiment processor initialized")

    def process_comment(self, connection, row_key, comment_text=None):
        """
        Analyze comment sentiment and update storage with the result.

        Coordinates the sentiment analysis workflow by either using provided text
        or fetching it from storage, running analysis, and persisting the result.

        Args:
            connection: HBase connection object
            row_key (str): Unique identifier for the comment
            comment_text (str, optional): Text to analyze. If None, fetches from HBase.

        Returns:
            str: Sentiment label (e.g., "Positive", "Negative") or None if processing failed

        Note:
            The method will fetch the comment from HBase if comment_text is None,
            making it flexible for both new comments and reprocessing existing ones.
        """
        # Only fetch from HBase if no text provided - optimizes typical flow
        if comment_text is None:
            comment_data = get_comment(connection, row_key)
            comment_text = comment_data.get('comment')

        if not comment_text:
            logger.warning(f"No comment text found for row_key: {row_key}")
            return None

        # Get sentiment prediction - method already handles batching if needed
        sentiment = self.analyzer.predict_sentiment(comment_text)[0]
        logger.debug(f"Analyzed sentiment for row_key {row_key}: {sentiment}")

        # Persist result to storage
        update_comment_with_sentiment(connection, row_key, sentiment)

        return sentiment