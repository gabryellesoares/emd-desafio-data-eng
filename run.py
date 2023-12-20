from pipelines.flows import flow
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()
    flow.run()