from kafka import KafkaProducer
import json
import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import sys
from config import KafkaConfig, YouTubeConfig, logger

def create_youtube_client():
    """
    Create and return a YouTube API client.

    Returns:
        googleapiclient.discovery.Resource: YouTube API client
    """
    try:
        if not YouTubeConfig.API_KEY:
            raise ValueError("YouTube API key not found in configuration")

        youtube = build('youtube', 'v3', developerKey=YouTubeConfig.API_KEY)
        logger.info("YouTube API client created successfully")
        return youtube
    except Exception as e:
        logger.error(f"Error creating YouTube client: {e}")
        raise

def get_live_chat_id(youtube, video_id):
    """
    Get the live chat ID for a given video.

    Args:
        youtube: YouTube API client
        video_id (str): ID of the YouTube video

    Returns:
        str: Live chat ID if available
    """
    try:
        video_response = youtube.videos().list(
            part='liveStreamingDetails',
            id=video_id
        ).execute()

        if not video_response['items']:
            raise ValueError(f"Video {video_id} not found or is not live")

        if 'liveStreamingDetails' not in video_response['items'][0]:
            raise ValueError(f"Video {video_id} is not a live stream")

        live_chat_id = video_response['items'][0]['liveStreamingDetails'].get('activeLiveChatId')
        if not live_chat_id:
            raise ValueError(f"No active live chat found for video {video_id}")

        logger.info(f"Retrieved live chat ID: {live_chat_id}")
        return live_chat_id
    except Exception as e:
        logger.error(f"Error getting live chat ID: {e}")
        raise

def create_producer():
    """
    Create and configure a Kafka producer.

    Returns:
        KafkaProducer: Configured Kafka producer instance
    """
    return KafkaProducer(
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3  # Add retries for better resilience
    )

def send_message(producer, message):
    """
    Send a message to the Kafka topic with error handling.

    Args:
        producer (KafkaProducer): Kafka producer instance
        message (dict): Message to be sent to Kafka
    """
    future = producer.send(KafkaConfig.TOPIC, message)
    try:
        future.get(timeout=KafkaConfig.PRODUCER_TIMEOUT)
        logger.debug(f"Message sent successfully: {message}")
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        raise  # Re-raise to handle in the calling function

def process_chat_messages(youtube, live_chat_id, producer, next_page_token=None):
    """
    Retrieve and process live chat messages.

    Args:
        youtube: YouTube API client
        live_chat_id (str): ID of the live chat
        producer (KafkaProducer): Kafka producer instance
        next_page_token (str, optional): Token for the next page of results

    Returns:
        tuple: (next_page_token, messages_processed)
    """
    messages_processed = 0
    try:
        chat_response = youtube.liveChatMessages().list(
            liveChatId=live_chat_id,
            part='snippet,authorDetails',
            maxResults=YouTubeConfig.MAX_RESULTS,
            pageToken=next_page_token
        ).execute()

        for chat_message in chat_response['items']:
            try:
                # Extract required fields with safety checks
                message_data = {
                    'user_id': chat_message['authorDetails']['channelId'],
                    'comment': chat_message['snippet']['displayMessage'],
                    'timestamp': chat_message['snippet']['publishedAt']
                }

                # Only process non-empty messages
                if message_data['comment'].strip():
                    send_message(producer, message_data)
                    messages_processed += 1

            except KeyError as e:
                logger.warning(f"Missing required field in chat message: {e}")
                continue
            except Exception as e:
                logger.error(f"Error processing individual message: {e}")
                continue

        return chat_response.get('nextPageToken'), messages_processed
    except HttpError as e:
        if e.resp.status in [403, 429]:  # Quota exceeded or rate limited
            logger.error(f"YouTube API quota exceeded or rate limited: {e}")
            time.sleep(YouTubeConfig.POLL_INTERVAL * 2)  # Wait longer before retry
        raise
    except Exception as e:
        logger.error(f"Error processing chat messages: {e}")
        return next_page_token, messages_processed

def main():
    """
    Main function to process YouTube live chat messages.
    """
    if len(sys.argv) != 2:
        print("Usage: python producer.py <youtube_video_id>")
        sys.exit(1)

    video_id = sys.argv[1]
    youtube = create_youtube_client()
    producer = create_producer()
    error_count = 0
    max_errors = 5

    try:
        live_chat_id = get_live_chat_id(youtube, video_id)
        next_page_token = None
        logger.info(f"Starting to monitor live chat for video: {video_id}")

        while True:
            try:
                next_page_token, messages_processed = process_chat_messages(
                    youtube,
                    live_chat_id,
                    producer,
                    next_page_token
                )

                if messages_processed > 0:
                    logger.info(f"Processed {messages_processed} messages")
                    error_count = 0  # Reset error count on successful processing

                sleep_time = YouTubeConfig.POLL_INTERVAL
                if not next_page_token:
                    sleep_time *= 2  # Wait longer if no more messages

                time.sleep(sleep_time)

            except Exception as e:
                error_count += 1
                logger.error(f"Error in main loop (attempt {error_count}/{max_errors}): {e}")

                if error_count >= max_errors:
                    logger.error("Maximum error count reached, stopping producer")
                    break

                time.sleep(YouTubeConfig.POLL_INTERVAL * 2)  # Wait longer before retry

    except KeyboardInterrupt:
        logger.info("\nStopping producer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        producer.close()
        logger.info("Producer stopped and resources cleaned up.")

if __name__ == "__main__":
    main()