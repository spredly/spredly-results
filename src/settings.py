import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

class Settings(BaseSettings):

    # rabbitmq
    RABBIT_HOST: str = os.environ.get("RABBITMQ_HOST")
    RABBIT_PORT: int = os.environ.get("RABBITMQ_PORT")
    RABBIT_USER: str = os.environ.get("RABBITMQ_USER")
    RABBIT_PASSWORD: str = os.environ.get("RABBITMQ_PASS")

settings = Settings(_case_sensitive=False)
