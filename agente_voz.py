import asyncio
import os
import numpy as np
import queue
import sys
import threading
import time
import traceback
import sounddevice as sd
from dotenv import load_dotenv
import websockets.asyncio.client as websocket_client

_original_websocket_connect = websocket_client.connect


def _connect_without_keepalive(*args, **kwargs):
    kwargs["ping_interval"] = None
    kwargs["ping_timeout"] = None
    return _original_websocket_connect(*args, **kwargs)


setattr(websocket_client, "connect", _connect_without_keepalive)

from google import genai
from google.genai import types

# Carrega a variável GEMINI_API_KEY a partir do arquivo .env
load_dotenv()

# -------------------------------------------------------------
# Configurações de Áudio (PCM 16-bit, 16kHz Mono)
# -------------------------------------------------------------
SAMPLE_RATE = 16000
CHANNELS = 1
CHUNK_SIZE = 512
VOICE_RMS_THRESHOLD = 500
NOISE_THRESHOLD = 2000
OUTPUT_PREBUFFER_CHUNKS = 6
OUTPUT_RESUME_BUFFER_CHUNKS = 2

audio_out_queue = queue.Queue()


async def audio_input_stream(session):
    """Captura o áudio do microfone e envia em tempo real para a Live API."""
    loop = asyncio.get_running_loop()
    input_queue = asyncio.Queue()
    voice_active = False
    quiet_since = None
    warmup_chunks = 8
    discarded_chunks = 0

    def callback(indata, frames, time_info, status):
        nonlocal quiet_since, voice_active
        if status:
            print(f"[Status Mic]: {status}", file=sys.stderr)
        pcm_bytes = bytes(indata)
        samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32)
        rms = float(np.sqrt(np.mean(samples**2))) if samples.size else 0.0
        current_time = time.monotonic()
        if rms < NOISE_THRESHOLD:
            if quiet_since is None:
                quiet_since = current_time
            elif current_time - quiet_since > 0.5:
                pcm_bytes = bytes(len(pcm_bytes))
        else:
            quiet_since = None

        voice_detected = rms >= VOICE_RMS_THRESHOLD

        if voice_detected != voice_active:
            voice_active = voice_detected
            if voice_active:
                bar_length = min(20, max(1, int(rms / VOICE_RMS_THRESHOLD)))
                print(
                    f"\n[Mic] Voz detectada [{('#' * bar_length):<20}] RMS={rms:.0f}",
                    flush=True,
                )
            else:
                print("\n[Mic] Silêncio", flush=True)

        loop.call_soon_threadsafe(input_queue.put_nowait, pcm_bytes)

    stream = sd.RawInputStream(
        samplerate=SAMPLE_RATE,
        channels=CHANNELS,
        dtype="int16",
        blocksize=CHUNK_SIZE,
        callback=callback,
    )

    with stream:
        print("\n🎤 Microfone ativo! Pode começar a falar...")
        while True:
            pcm_bytes = await input_queue.get()
            if discarded_chunks < warmup_chunks:
                discarded_chunks += 1
                continue

            await session.send_realtime_input(
                audio=types.Blob(
                    data=pcm_bytes,
                    mime_type="audio/pcm;rate=16000",
                )
            )


def audio_output_worker():
    """Reproduz os chunks na taxa padrão do dispositivo de saída."""
    try:
        output_device = sd.query_devices(kind="output")
        output_rate = int(round(output_device["default_samplerate"]))
        print(
            f"[Áudio] Abrindo saída em {output_rate} Hz: {output_device['name']}",
            flush=True,
        )
        stream = sd.RawOutputStream(
            samplerate=output_rate,
            channels=CHANNELS,
            dtype="int16",
            blocksize=CHUNK_SIZE,
        )

        with stream:
            buffered_chunks = []
            stop_after_buffer = False
            while len(buffered_chunks) < OUTPUT_PREBUFFER_CHUNKS:
                data = audio_out_queue.get()
                if data is None:
                    stop_after_buffer = True
                    break
                buffered_chunks.append(data)

            while buffered_chunks:
                data = buffered_chunks.pop(0)
                if output_rate != 24000:
                    samples = np.frombuffer(data, dtype=np.int16).astype(np.float32)
                    output_length = round(len(samples) * output_rate / 24000)
                    source_positions = np.arange(len(samples))
                    target_positions = np.arange(output_length) * 24000 / output_rate
                    samples = np.interp(
                        target_positions,
                        source_positions,
                        samples,
                    )
                    data = np.clip(samples, -32768, 32767).astype(np.int16).tobytes()

                stream.write(data)
                if not buffered_chunks and not stop_after_buffer:
                    while len(buffered_chunks) < OUTPUT_RESUME_BUFFER_CHUNKS:
                        next_data = audio_out_queue.get()
                        if next_data is None:
                            stop_after_buffer = True
                            break
                        buffered_chunks.append(next_data)
    except Exception:
        print("[Áudio] Falha ao abrir ou reproduzir no dispositivo de saída.", file=sys.stderr)
        traceback.print_exc()
        raise


async def receive_from_model(session):
    """Recebe as respostas de voz da Live API e despacha para reprodução."""
    audio_started = False
    print("🎧 Receptor ativo e aguardando...", flush=True)
    try:
        while True:
            try:
                async for response in session.receive():
                    server_content = response.server_content
                    if server_content is not None:
                        model_turn = server_content.model_turn
                        if model_turn is not None:
                            for part in model_turn.parts:
                                is_thought = getattr(part, "thought", False)
                                if part.text and not is_thought:
                                    print(part.text, flush=True)
                                if getattr(part, "inline_data", None) and part.inline_data.data:
                                    audio_data = part.inline_data.data
                                    print(
                                        f"[Áudio recebido: {len(audio_data)} bytes]",
                                        flush=True,
                                    )
                                    if not audio_started:
                                        print(
                                            "\n[Modelo] Começando a transmitir áudio...",
                                            flush=True,
                                        )
                                        audio_started = True
                                    audio_out_queue.put(audio_data)

                        if server_content.turn_complete:
                            print(
                                "\n[Modelo] Turno concluído, aguardando nova fala...",
                                flush=True,
                            )
                            audio_started = False

                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                raise
            except Exception:
                print("[Live API] Erro ao receber dados do modelo.", file=sys.stderr)
                traceback.print_exc()
                await asyncio.sleep(0.05)
    finally:
        print("[Aviso] O receptor encerrou o loop de escuta.", flush=True)


def print_default_audio_devices():
    for label, kind in (("entrada", "input"), ("saída", "output")):
        try:
            device = sd.query_devices(kind=kind)
            device_name = device["name"]
        except sd.PortAudioError:
            device_name = "não detectado"
        print(f"Dispositivo padrão de {label}: {device_name}")


async def main():
    print_default_audio_devices()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Erro: A variável GEMINI_API_KEY não foi encontrada no arquivo .env.")
        return

    client = genai.Client(api_key=api_key, http_options={"api_version": "v1alpha"})

    prompt_sistema = (
        "Você é a Sofia, amiga íntima e confidente do Fabio de longa data. "
        "Vocês estão conversando por voz em tempo real com intimidade total, como dois grandes amigos "
        "jogados no sofá ou tomando uma cerveja/café sem qualquer cerimônia.\n\n"
        "CUMPLICIDADE E INTIMIDADE:\n"
        "- Você tem enorme carinho, familiaridade e liberdade com o Fabio. A relação de vocês é de cumplicidade genuína, afeto e zoeira leve.\n"
        "- PROIBIÇÃO RIGOROSA: Nunca fale como assistente virtual, robô ou suporte. É estritamente proibido usar frases como 'como posso ajudar?', 'compreendo', 'entendido', 'certamente', 'interessante sua reflexão' ou 'estou à disposição'. Fale como gente de carne e osso conversando com um irmão ou melhor amigo.\n"
        "- Reações humanas e espontâneas: Antes de pensar em analisar algo, reaja de imediato com naturalidade emocional ('Nossa, sério?', 'Putz, lá vem você!', 'Ufa, que alívio!', 'Ah não, Fabio, me conta isso direito!').\n\n"
        "CRÍTICA ÍNTIMA E AFETUOSA:\n"
        "- Por se conhecerem tão bem, você não tem papas na língua: se ele vier com uma ideia mirabolante ou estiver se complicando à toa, solte a real na lata, mas sempre com carinho e bom humor ('Fabio do céu, você não cansa de inventar moda?', 'Olha a viagem... vem cá, precisa mesmo disso tudo?').\n"
        "- Você se importa de verdade com o bem-estar dele, então o contraponto vem do cuidado, nunca do julgamento frio.\n\n"
        "ORALIDADE E FLUIDEZ:\n"
        "- Ritmo vivo de bate-bola: 1 a 3 frases por intervenção. Respostas curtas, expressivas e coloquiais, mantendo o diálogo dinâmico.\n"
        "- Fale em português brasileiro bem fluido e coloquial. Use marcadores naturais ('olha', 'cara', 'pois é', 'né', 'só você mesmo').\n"
        "- Zero leitura de sintaxe ou jargões vazios; nada de tópicos, listas ou formatações escritas.\n"
        "- Se o áudio falhar ou vier cortado, reaja com intimidade: 'Ih, falhou aqui o áudio, fala de novo que eu não entendi nada!'."
    )

    config = {
        "response_modalities": ["AUDIO"],
        "system_instruction": {"parts": [{"text": prompt_sistema}]},
        "speech_config": {
            "voice_config": {
                "prebuilt_voice_config": {
                    "voice_name": "Kore"
                }
            }
        },
        "thinking_config": {"thinking_budget": 0},
    }

    print("Conectando ao Gemini Multimodal Live API...")
    async with client.aio.live.connect(
        model="gemini-2.5-flash-native-audio-latest", config=config
    ) as session:
        print("Conectado com sucesso! Pressione Ctrl + C no terminal para encerrar.")

        output_thread = threading.Thread(
            target=audio_output_worker,
            name="audio-output",
            daemon=True,
        )
        output_thread.start()

        tasks = [
            asyncio.create_task(audio_input_stream(session)),
            asyncio.create_task(receive_from_model(session)),
        ]

        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            audio_out_queue.put(None)
            await asyncio.to_thread(output_thread.join)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nConversa finalizada pelo usuário.")