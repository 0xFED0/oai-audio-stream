from flask import Flask, Response, request, render_template
from dotenv import load_dotenv, find_dotenv
from openai import OpenAI, Stream
from openai.types.chat.chat_completion import ChatCompletion

from dataflow import GeneratorDataFlowThread, DataFlowThread

import os
import io
import re
from time import time, sleep
from collections import deque

load_dotenv(find_dotenv())

app = Flask(__name__)
running = True

oai_client = OpenAI()


delimiters = re.compile(r'[\.\,\!\?\;\:\n]+')

speech_index = 0
speeches = []
CHUNK = 5 * 1024


def numbers(last: int, n: int):
    for i in range(n):
        yield "%d. " % (last + i + 1)


def streamed_gpt(df: DataFlowThread, messages):
    stream: Stream = oai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=messages,
        n=1,
        stream=True,
        temperature=0.5,
        max_tokens=1000,
        timeout=30,
    )
    for response in stream:
        delta = response.choices[0].delta
        if not running or df.queue.full():
            stream.response.close()
            return delta.content
        yield delta.content
    print("GPT ended")


def sentences_it(text_it):
    sentence = ""
    for s in text_it:
        m = delimiters.match(s)
        if m:
            end = m.end()
            is_num = (s[end - 1] in (".", ",")) and end < len(s) and s[end].isdigit()
            if not is_num:
                sentence += s[:end]
                yield sentence.strip()
                sentence = s[end:]
        else:
            sentence += s
    return sentence.strip()


def gen_meditation(df: DataFlowThread):
    instruction = {"role": "user", "content": "Make and continue infinity meditation text!"}
    history = deque([], maxlen=7)
    idx = 0
    st_time = time()
    print("START GPT")
    while running:
        messages = [instruction]
        if len(history) > 0:
            messages.append({"role": "assistant", "content": " ".join(history)})
        print("GPT REQUEST")
        for content in sentences_it(streamed_gpt(df, messages)):
            history.append(content)
            elapsed = time() - st_time
            st_time = time()
            idx += 1
            print("[%d][%.3fs]>" % (idx, elapsed), content)
            if df.queue.full():
                print("FULL GPT QUEUE")
            yield content
    pass


def gen_speeches(df: DataFlowThread):
    print("START GEN SPEECHES", df.name)
    for text in df.iter_input():
        if df.queue.full():
            print("FULL TTS QUEUE")
        response = oai_client.audio.speech.create(
                model="tts-1",
                voice="onyx",
                speed=0.9,
                response_format="opus",
                input=text,
            )
        yield from response.iter_bytes(CHUNK)
        yield None


ai_thread = GeneratorDataFlowThread(func=gen_meditation, max_queue=3, name="AI")
tts_thread = GeneratorDataFlowThread(func=gen_speeches, max_queue=(32*1024 // CHUNK), name="TTS")
tts_thread.set_input(ai_thread)


def sound():
    for chunk in tts_thread:
        if chunk is None:
            break
        yield chunk


@app.route('/audio')
def audio():
    connection = Response(sound(),
                          mimetype="audio/opus", status=200)
    return connection


@app.route('/user/speech', methods=["POST"])
def user_speech():
    file = io.BytesIO(request.stream.read())
    file.name = "speech.wav"
    st = time()
    trans = oai_client.audio.transcriptions.create(
        model="whisper-1",
        file=file,
        response_format="text"
    )
    print("RESPOND", time() - st)
    print("TRANSCRIPTION: ", trans)

    return Response(status=200)


@app.route('/user/wait', methods=["POST"])
def user_wait():
    pass


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, threaded=True, port=5000)
    running = False
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        ai_thread.join(timeout=3)
        tts_thread.join(timeout=3)
