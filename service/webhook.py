import os
from time import time
import requests
import json


def _get_required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


URL = _get_required_env("WEBHOOK_URL")
# curl -X POST "https://defaultb0e1d4a797424ff78a34f7e6a83849.4a.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/f96fbcb41fdc430684bb1a96afbb5284/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=cxeFmj6mLbRJrgASYXCT4GXQXVlXxHuhRkpoPGUNv_g" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "type": "message",
#     "attachments": [
#       {
#         "contentType": "application/vnd.microsoft.card.adaptive",
#         "content": {
#           "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
#           "type": "AdaptiveCard",
#           "version": "1.4",
#           "body": [
#             {
#               "type": "Container",
#               "style": "attention",
#               "items": [
#                 {
#                   "type": "TextBlock",
#                   "text": "🚨 안전안내",
#                   "weight": "Bolder",
#                   "size": "Large"
#                 }
#               ]
#             },
#             {
#               "type": "FactSet",
#               "facts": [
#                 {
#                   "title": "재난 유형",
#                   "value": "대설"
#                 },
#                 {
#                   "title": "메시지 구분",
#                   "value": "안전안내"
#                 }
#               ]
#             },
#             {
#               "type": "TextBlock",
#               "text": "현재 지역에 대설 경보가 발효되었습니다.\n외출을 자제하고 대중교통을 이용하시기 바랍니다.\n시설물 붕괴에 주의하십시오.",
#               "wrap": true,
#               "spacing": "Medium"
#             }
#           ]
#         }
#       }
#     ]
#   }'

# Label	Emotional Intensity	Recommended Emoji
# 긴급재난	High / Immediate	🚨
# 안전안내	Moderate / Informational	ℹ️



def build_body(type: str, title: str, facts: dict[str, str], message: str) -> dict:
    if type == "긴급재난":
        emoji = "🚨"
    elif type == "안전안내":
        emoji = "ℹ️"
    else:
        emoji = ""
    attachments = [
        {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "Container",
                        "style": "attention",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": f"{emoji} {title}",
                                "weight": "Bolder",
                                "size": "Large",
                            }
                        ],
                    },
                    {
                        "type": "FactSet",
                        "facts": [{"title": k, "value": v} for k, v in facts.items()],
                    },
                    {
                        "type": "TextBlock",
                        "text": message,
                        "wrap": True,
                        "spacing": "Medium",
                    },
                ],
            },
        }
    ]

    return {"type": "message", "attachments": attachments}


def send_webhook(type: str, title: str, facts: dict[str, str], message: str) -> None:

    body = build_body(type, title, facts, message)
    headers = {"Content-Type": "application/json"}

    print("Sending webhook with body:", json.dumps(body, ensure_ascii=False, indent=2))
    response = requests.post(URL, json=body, headers=headers)
    response.raise_for_status()


if __name__ == "__main__":
    send_webhook(
        type="안전안내",
        title="안전안내",
        facts={
            "재난 유형": "대설",
            "메시지 구분": "안전안내",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
        message="현재 지역에 대설 경보가 발효되었습니다.\n외출을 자제하고 대중교통을 이용하시기 바랍니다.\n시설물 붕괴에 주의하십시오.",
    )
