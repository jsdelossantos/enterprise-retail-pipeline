# 1. Get the foundation: Start with a lightweight version of Linux that has Python 3.11 pre-installed
FROM python:3.11-slim

# 2. Create a folder inside the container called /app and move inside it
WORKDIR /app

# 3. Copy our "shopping list" from your laptop into the container first
COPY requirements.txt .

# 4. Tell the container to install the libraries on the list
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy ALL the rest of your files (the src folder, sql folder, data folder) into the container
COPY . .

# 6. The final instruction: Point Python to the correct folder path!
CMD ["python", "src/pipeline.py"]