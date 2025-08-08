"""
AWS Lambda queue function that processes messages from SQS.
"""

import config  # Import config for environment variables
import json
from urllib.parse import parse_qs
from logger_setup import get_logger
from typing import Dict, Any, List
from handlers.queue_message_handler import process_message

logger = get_logger("queue")

def parse_message_body(body: str) -> Dict[str, Any]:
    """
    Parse URL-encoded message body into structured data.

    Args:
        body: URL-encoded message string

    Returns:
        Parsed message data as dictionary
    """
    try:
        # Parse URL-encoded string to dict
        parsed = parse_qs(body)
        logger.debug("Parsed message body", parsed_data=parsed)

        # Convert single-item lists to scalar values
        data = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}

        # Parse channel metadata if present
        if "ChannelMetadata" in data:
            try:
                data["ChannelMetadata"] = json.loads(data["ChannelMetadata"])
                logger.debug(
                    "Parsed channel metadata", metadata=data["ChannelMetadata"]
                )
            except json.JSONDecodeError as e:
                logger.warning(
                    "Failed to parse ChannelMetadata",
                    error=str(e),
                    metadata=data["ChannelMetadata"],
                )

        # Initialize media list
        media_items = []
        media_count = int(data.get("NumMedia", 0))

        # If media is present, collect all media items
        for i in range(media_count):
            media_items.append(
                {
                    "url": data.get(f"MediaUrl{i}"),
                    "content_type": data.get(f"MediaContentType{i}"),
                }
            )
            logger.debug(f"Added media item {i}", media_item=media_items[-1])

        parsed_data = {
            "message_type": data.get("MessageType", "unknown"),
            "channel": "whatsapp",  # Based on the To/From format
            "sender": {
                "id": data.get("WaId"),
                "name": data.get("ProfileName"),
                "phone": data.get("From", "").replace("whatsapp:", ""),
            },
            "content": {
                "text": data.get("Body", ""),
                "media_count": media_count,
                "media_items": media_items,
                "segments": int(data.get("NumSegments", 1)),
            },
            "metadata": {
                "message_id": data.get("MessageSid"),
                "account_id": data.get("AccountSid"),
                "status": data.get("SmsStatus"),
                "channel_data": data.get("ChannelMetadata", {}),
            },
            "raw": data,  # Keep original data for reference
        }
        logger.debug("Successfully parsed message", parsed_data=parsed_data)
        return parsed_data

    except Exception as e:
        logger.error("Error parsing message body", error=str(e), body=body)
        raise


def group_messages_by_sender(
    records: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group messages by sender ID and parse message bodies.

    Args:
        records: Raw SQS records

    Returns:
        Dict mapping sender IDs to their parsed messages
    """
    messages_by_sender = {}
    logger.info("Starting to group messages", record_count=len(records))

    for record in records:
        try:
            # Parse the message body
            parsed_body = parse_message_body(record["body"])
            sender_id = parsed_body["sender"]["id"]

            if sender_id not in messages_by_sender:
                messages_by_sender[sender_id] = []
                logger.debug("Created new sender group", sender_id=sender_id)

            messages_by_sender[sender_id].append(
                {
                    "message_id": parsed_body["metadata"]["message_id"],
                    "body": parsed_body,
                }
            )
            logger.debug(
                "Added message to sender group",
                sender_id=sender_id,
                message_id=parsed_body["metadata"]["message_id"],
            )

        except Exception as e:
            logger.error(
                "Error processing message",
                error=str(e),
                message_id=record.get("messageId", "UNKNOWN"),
                record=record,
            )
            continue

    logger.info(
        "Finished grouping messages",
        sender_count=len(messages_by_sender),
        total_messages=sum(len(msgs) for msgs in messages_by_sender.values()),
    )
    return messages_by_sender


def queue_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function for processing SQS FIFO messages.
    Messages are grouped by senderId and processed in batches.

    Args:
        event: The event dict that contains the SQS records
        context: The context object that contains information about the runtime

    Returns:
        Dict containing the processing results
    """
    try:
        logger.info("Starting queue handler", request_id=context.aws_request_id)
        if event.get("source") == "aws.events":
            logger.info("Received warm-up event from CloudWatch")
            return {
                "statusCode": 200,
                "body": "Warmed up!",
            }
            
        records = event.get("Records", [])

        if not records:
            logger.info("No messages in event")
            return {"statusCode": 200, "body": "No messages to process"}

        logger.info("Processing batch of messages", message_count=len(records))

        # Group and parse messages by sender
        messages_by_sender = group_messages_by_sender(records)

        # Process each sender's messages
        for sender_id, messages in messages_by_sender.items():
            try:
                logger.info(
                    "Processing messages for sender",
                    sender_id=sender_id,
                    message_count=len(messages),
                )
                process_message(sender_id, messages)
                logger.info(
                    "Successfully processed messages for sender",
                    sender_id=sender_id,
                    message_count=len(messages),
                )
            except Exception as e:
                logger.error(
                    "Failed to process messages for sender",
                    error=str(e),
                    sender_id=sender_id,
                    message_count=len(messages),
                )

        logger.info(
            "Batch processing complete",
            total_processed=len(records),
            sender_count=len(messages_by_sender),
        )
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Batch processing complete",
                    "total_processed": len(records),
                    "sender_count": len(messages_by_sender),
                }
            ),
        }

    except Exception as e:
        logger.error(
            "Error in queue handler", error=str(e), request_id=context.aws_request_id
        )
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error", "message": str(e)}),
        }
