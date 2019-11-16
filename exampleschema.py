

def get_telegram_media_schema():
    return {"properties": {
        "type": {"type": "string", "maxLength": 280},
        "mime-type": {"type": "string", "maxLength": 280},
        "file_name": {"type": "string", "maxLength": 280},
        "path": {"type": "string", "maxLength": 280},
        "extracted-text": {"type": "string"},
    }}


def get_doc():
    return {
        "type": "mediadocument",
        "mime-type": "application/pdf",
        "file_name": "testfile",
        "path": "data/testfile",
        "extracted-text": "hallo welt",
    }