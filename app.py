from dotenv import load_dotenv
import os

load_dotenv()

from app import create_app

app = create_app()

if __name__ == "__main__":
	app.run(host="127.0.0.1", port=8000, debug=True)
