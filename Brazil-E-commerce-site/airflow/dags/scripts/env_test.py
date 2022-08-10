from dotenv import load_dotenv
import os


load_dotenv = load_dotenv()

my_id = os.getenv("ACCESS_NAME")

print(my_id)

