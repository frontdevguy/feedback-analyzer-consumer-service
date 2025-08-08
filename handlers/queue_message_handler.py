"""
Message processor for handling batched messages from SQS.
"""

import boto3
import time
import uuid
from logger_setup import get_logger
from typing import Dict, Any, List

logger = get_logger("message_processor")

dynamodb = boto3.resource("dynamodb")
session_table = dynamodb.Table("sessions")
chat_table = dynamodb.Table("chats")


def store_chat_messages(
    session_id: str, sender_id: str, messages: List[Dict[str, Any]]
) -> None:
    """
    Store chat messages in DynamoDB using batch writes in chunks of 25 messages.

    Args:
        session_id: The ID of the session
        sender_id: The ID of the sender
        messages: List of parsed messages to store
    """
    try:
        timestamp = int(time.time())

        # Process messages in chunks of 25 (DynamoDB batch write limit)
        for i in range(0, len(messages), 25):
            chunk = messages[i : i + 25]
            batch_items = []

            for message in chunk:
                parsed_body = message["body"]  # Already parsed in consumer
                content = parsed_body["content"]

                # Prepare content data including both text and media
                message_content = {
                    "text": content["text"],
                    "media_count": content["media_count"],
                    "segments": content["segments"],
                }

                # Add media items if present
                if content.get("media_items"):
                    message_content["media_items"] = content["media_items"]

                # Create PutRequest item
                batch_items.append(
                    {
                        "PutRequest": {
                            "Item": {
                                "sender_id": sender_id,
                                "chat_type": "inbound",
                                "session_id": session_id,
                                "message_id": parsed_body["metadata"]["message_id"],
                                "content": message_content,
                                "sender_info": parsed_body["sender"],
                                "metadata": parsed_body["metadata"],
                                "created_at": timestamp,
                            }
                        }
                    }
                )

            if batch_items:
                # Execute batch write for the current chunk
                response = chat_table.meta.client.batch_write_item(
                    RequestItems={chat_table.name: batch_items}
                )

                # Handle unprocessed items if any
                unprocessed = response.get("UnprocessedItems", {}).get(
                    chat_table.name, []
                )
                if unprocessed:
                    logger.warning(
                        "Some items were not processed",
                        unprocessed_count=len(unprocessed),
                        sender_id=sender_id,
                        chunk_size=len(chunk),
                    )

        logger.info(
            "Stored chat messages",
            message_count=len(messages),
            session_id=session_id,
            sender_id=sender_id,
        )

    except Exception as e:
        logger.error(
            "Error storing chat messages",
            error=e,
            session_id=session_id,
            sender_id=sender_id,
        )
        raise


def get_or_create_session(sender_id: str) -> str:
    """
    Get active session for sender or create new one.

    Args:
        sender_id: The ID of the sender

    Returns:
        session_id: The ID of the active or new session
    """
    try:
        # Check for existing active session
        response = session_table.query(
            IndexName="SenderSessionsIndex",
            KeyConditionExpression="sender_id = :sid",
            FilterExpression="#status = :status",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":sid": sender_id, ":status": "active"},
            Limit=1,
            ScanIndexForward=False,  # Latest first
        )

        if response["Items"]:
            return response["Items"][0]["session_id"]

        session_id = str(uuid.uuid4())

        session_table.put_item(
            Item={
                "session_id": session_id,
                "sender_id": sender_id,
                "status": "active",
                "created_at": int(time.time()),
            }
        )

        logger.info("Created new session", session_id=session_id, sender_id=sender_id)

        return session_id

    except Exception as e:
        logger.error("Error managing session", error=e, sender_id=sender_id)
        raise


def process_message(sender_id: str, messages: List[Dict[str, Any]]) -> None:
    """
    Process a batch of messages for a sender.

    Args:
        sender_id: The ID of the sender
        messages: List of parsed messages to process
    """
    try:
        session_id = get_or_create_session(sender_id)
        store_chat_messages(session_id, sender_id, messages)
    except Exception as e:
        logger.error("Error processing messages", error=e, sender_id=sender_id)
        raise
