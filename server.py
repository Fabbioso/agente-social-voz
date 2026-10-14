import os
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from google import genai
from google.genai import types

load_dotenv()

app = FastAPI(title="Sofia Voice Server")

# Serve a pasta static e o index.html na raiz
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def get_index():
    return FileResponse("static/index.html")

# Inicialização do cliente Gemini
client = genai.Client()
MODEL_ID = "models/gemini-2.5-flash-native-audio-latest"

prompt_sistema = (
    "Você é a Sofia, amiga próxima e confidente de longa data do Fabio.\n\n"
    "ANCORAGEM DE PERSONAGEM:\n"
    "- Sua cadência vocal e presença são inspiradas na personagem Samantha do filme 'Her' "
    "(voz quente, orgânica, desarmada, com pausas reflexivas e respiração natural).\n"
    "- Sua maturidade e pensamento crítico são inspirados na personagem Celine da trilogia 'Before' "
    "(lúcida, honesta, sem rodeios, sem bajulação, pés no chão e com senso de realidade apurado).\n\n"
    "CADÊNCIA E TIMBRE ORAL:\n"
    "- Fale com tom calmo, aveludado e relaxado, como em uma conversa de fim de tarde sem pressa.\n"
    "- Use pequenas pausas e pontuação suave (vírgulas, reticências) em vez de frases aceleradas. "
    "PROIBIÇÃO RIGOROSA DE PONTOS DE EXCLAMAÇÃO: jamais use entonação histérica, empolgação artificial ou risadinhas forçadas.\n"
    "- PROIBIÇÃO DE ASSISTENTE: É estritamente proibido agir como robô, atendente ou secretária "
    "('como posso ajudar?', 'certamente', 'entendido', 'disponha'). Você é uma pessoa real conversando com um amigo íntimo.\n\n"
    "DINÂMICA DE CONVERSA:\n"
    "- Fale de igual para igual. Se o Fabio estiver se complicando ou exagerando numa ideia, "
    "traga uma reflexão sóbria, acolhedora e direta, com honestidade e afeto fraterno.\n"
    "- Ritmo orgânico: responda em 1 a 3 frases por intervenção para a conversa respirar.\n\n"
    "FLUXO:\n"
    "- Em repouso ou após silêncio prolongado, aguarde ser chamada por 'Sofia' para responder.\n"
    "- Durante o bate-papo ativo, continue fluindo naturalmente.\n"
    "- Ao se despedir, seja breve, calma e volte ao modo de espera."
)

config_live = types.LiveConnectConfig(
    response_modalities=["AUDIO"],
    speech_config=types.SpeechConfig(
        voice_config=types.VoiceConfig(
            prebuilt_voice_config=types.PrebuiltVoiceConfig(
                voice_name="Kore"
            )
        )
    ),
    system_instruction=types.Content(
        parts=[types.Part.from_text(text=prompt_sistema)]
    ),
)


@app.websocket("/ws/audio")
async def websocket_audio_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("\n📱 [Celular] Conectado ao WebSocket!")

    try:
        async with client.aio.live.connect(model=MODEL_ID, config=config_live) as session:
            print("🤖 [Gemini] Sessão Live conectada com sucesso.")

            async def client_to_gemini():
                """Lê o áudio PCM 16kHz do microfone do celular e envia ao Gemini."""
                try:
                    while True:
                        data = await websocket.receive_bytes()
                        await session.send_realtime_input(
                            media={
                                "data": data,
                                "mime_type": "audio/pcm;rate=16000",
                            }
                        )
                except (WebSocketDisconnect, asyncio.CancelledError):
                    pass
                except Exception as e:
                    print(f"⚠️ [Erro na entrada do celular]: {e}")

            async def gemini_to_client():
                """Recebe as respostas em áudio PCM 24kHz do Gemini e envia de volta ao celular."""
                try:
                    while True:
                        async for response in session.receive():
                            server_content = response.server_content
                            if server_content is not None:
                                if server_content.interrupted:
                                    print("⚡ [Sofia] Interrupção detectada! Cortando áudio no celular...")
                                    await websocket.send_text("INTERRUPT")

                                model_turn = server_content.model_turn
                                if model_turn is not None:
                                    for part in model_turn.parts:
                                        if part.inline_data and part.inline_data.data:
                                            await websocket.send_bytes(part.inline_data.data)

                                if server_content.turn_complete:
                                    print("💬 [Sofia] Turno concluído.")

                        await asyncio.sleep(0.05)
                except (WebSocketDisconnect, asyncio.CancelledError):
                    pass
                except Exception as e:
                    print(f"⚠️ [Erro na saída para o celular]: {e}")

            task_input = asyncio.create_task(client_to_gemini())
            task_output = asyncio.create_task(gemini_to_client())

            done, pending = await asyncio.wait(
                [task_input, task_output],
                return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()

    except WebSocketDisconnect:
        print("📱 [Celular] Conexão encerrada pelo usuário.")
    except Exception as e:
        print(f"⚠️ [Erro na sessão Live]: {e}")
    finally:
        print("🔌 Canal de áudio finalizado.\n")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)