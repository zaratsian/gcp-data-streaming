FROM apache/beam_python3.7_sdk:2.25.0

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT [ "python3", "main.py", "--input", "words.txt", "--output". "out.txt" ]


