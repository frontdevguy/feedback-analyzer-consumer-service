import os
import boto3
import httpx
from logger_setup import get_logger
from datetime import datetime
import config  # Ensure secrets are loaded into environment

logger = get_logger("reply")

dynamodb = boto3.resource("dynamodb")
session_table = dynamodb.Table("sessions")


async def notify_reply_service(sender_id: str) -> None:
    url = "https://intelligence.theuncproject.com/reply/"
    payload = {"sender_id": sender_id, "message": f"Hello, world! {sender_id}"}
    headers = {
        "x-intelligence-api-secret": os.environ.get("INTELLIGENCE_API_SECRET", "")
    }

    try:
        user_session = session_table.query(
            IndexName="SenderSessionsIndex",
            KeyConditionExpression="sender_id = :sid",
            FilterExpression="#status = :status",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":sid": sender_id, ":status": "active"},
            Limit=1,
            ScanIndexForward=False,  # Latest first
        )

        if user_session.get("Items") and len(user_session["Items"]) > 0:
            user_session_is_limited = user_session["Items"][0].get("user_limited_until")

            if user_session_is_limited:
                user_session_is_limited = datetime.fromisoformat(
                    user_session_is_limited
                )
                if user_session_is_limited > datetime.now():
                    logger.info(
                        f"User {sender_id} is rate limited until {user_session_is_limited}"
                    )
                    return

    except Exception as e:
        logger.error("Failed to get user session", sender_id=sender_id, error=str(e))
        # Don't return here - allow the service to continue even if session check fails

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(url, json=payload, headers=headers)

        if response.status_code == 200:
            logger.info("Successfully notified reply service", sender_id=sender_id)
        else:
            logger.warning(
                "Reply service returned non-200",
                sender_id=sender_id,
                status_code=response.status_code,
                response_body=response.text,
            )
    except Exception as e:
        logger.error(
            "Failed to notify reply service", sender_id=sender_id, error=str(e)
        )
