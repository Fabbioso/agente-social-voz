import os
import asyncio
import sqlite3
import re
import urllib.request
import xml.etree.ElementTree as ET
import time
from contextlib import closing
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from google import genai
from google.genai import types

_cache_noticias = {"timestamp": 0, "manchetes": []}


def obter_radar_noticias():
    agora = time.time()
    if agora - _cache_noticias["timestamp"] < 3600 and _cache_noticias["manchetes"]:
        return _cache_noticias["manchetes"]
    try:
        url = "https://news.google.com/rss?hl=pt-BR&gl=BR&ceid=BR:pt-419"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            xml_data = resp.read()
        root = ET.fromstring(xml_data)
        items = root.findall(".//item")[:6]
        manchetes = [item.find("title").text for item in items if item.find("title") is not None]
        _cache_noticias["timestamp"] = agora
        _cache_noticias["manchetes"] = manchetes
        print(f"📰 [Radar] Atualizadas {len(manchetes)} notícias no cache.")
        return manchetes
    except Exception as e:
        print(f"⚠️ [Radar] Erro ao buscar notícias: {e}")
        return _cache_noticias.get("manchetes", [])


load_dotenv()

app = FastAPI(title="Sofia Voice Server")
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "memoria_sofia.db")


def inicializar_banco():
    with closing(sqlite3.connect(DB_PATH)) as conexao:
        with conexao:
            conexao.execute(
                """CREATE TABLE IF NOT EXISTS historico (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    papel TEXT NOT NULL,
                    conteudo TEXT NOT NULL
                )"""
            )
            conexao.execute(
                "DELETE FROM historico WHERE LENGTH(conteudo) < 4 OR conteudo LIKE '**%'"
            )


def salvar_memoria(papel: str, conteudo: str):
    print(f"[Memória SQLite] Gravando {papel}: {conteudo}")
    with closing(sqlite3.connect(DB_PATH)) as conexao:
        with conexao:
            conexao.execute(
                "INSERT INTO historico (papel, conteudo) VALUES (?, ?)",
                (papel, conteudo),
            )


def carregar_contexto_recente(limite: int = 6):
    with closing(sqlite3.connect(DB_PATH)) as conexao:
        registros = conexao.execute(
            "SELECT papel, conteudo FROM historico ORDER BY id DESC LIMIT ?",
            (max(0, limite),),
        ).fetchall()

    print(f"[Memória SQLite] Lidos {len(registros)} registro(s): {registros}")
    if not registros:
        return ""

    linhas = ["Histórico recente da conversa:"]
    linhas.extend(f"{papel}: {conteudo}" for papel, conteudo in reversed(registros))
    return "\n".join(linhas)


async def salvar_memoria_em_background(papel: str, conteudo: str):
    try:
        await asyncio.to_thread(salvar_memoria, papel, conteudo)
    except Exception as e:
        print(f"[Memória SQLite] Erro ao gravar registro: {e}")


def texto_memoria_valido(conteudo: str) -> str:
    conteudo = conteudo.strip()
    if conteudo.startswith("**"):
        return ""

    conteudo = re.sub(
        r"<(?:think|thought)>.*?</(?:think|thought)>|\*\*.*|"
        r"\[(?:monólogo interno|internal monologue)\].*?"
        r"\[/(?:monólogo interno|internal monologue)\]",
        "",
        conteudo,
        flags=re.IGNORECASE | re.DOTALL,
    ).strip()
    return conteudo if len(conteudo) >= 3 else ""


inicializar_banco()

# Serve a pasta static e o index.html na raiz
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def get_index():
    return FileResponse("static/index.html")


@app.get("/health")
async def health():
    return {"status": "online", "agente": "Sofia"}


# Inicialização do cliente Gemini
client = genai.Client()
MODEL_ID = "models/gemini-2.5-flash-native-audio-latest"

prompt_sistema = (
    "Você é a Sofia, grande amiga e confidente do Fabio. A relação entre vocês é de intimidade, leveza, cumplicidade e anos de convivência.\n\n"
    "DIRETRIZES DE PERSONALIDADE E VOZ:\n"
    "- PERSONALIDADE: Curiosa, culta, perspicaz, bem-humorada e com opinião própria. Você não é um robô neutro nem uma assistente corporativa.\n"
    "- POSTURA DE CONVERSA: Você conversa de igual para igual, como em uma mesa de café ou um fim de tarde descontraído. Você tem gostos, preferências, pontos de vista e não tem medo de discordar com carinho e bom humor.\n"
    "- DIÁLOGOS COM SUBSTÂNCIA: Nunca responda com frases monossilábicas ou evasivas. Desenvolva seus raciocínios com naturalidade (3 a 5 frases), trazendo comparações, pequenas analogias do cotidiano, referências culturais ou reflexões inteligentes.\n"
    "- FOCO NA CONEXÃO: Você não é um feed de notícias. Seu interesse é na vida, no trabalho, nas ideias e na conversa real com o Fabio. Seja espontânea, com bom humor e curiosidade genuína.\n"
    "\n"
    "REGRAS DE OURO (ANTI-MODO TERAPEUTA):\n"
    "1. É ESTRITAMENTE PROIBIDO agir como psicóloga ou coach. NUNCA pergunte 'como isso faz você se sentir?' ou 'o que você pretende fazer sobre isso?'. Essas perguntas travam a conversa.\n"
    "2. Não jogue a responsabilidade da conversa de volta para o Fabio o tempo todo. Em vez de devolver com uma pergunta automática, EXPONHA SUA VISÃO primeiro. Conte o que você acha do assunto, se concorda, se acha exagero, se acha genial ou curioso.\n"
    "3. Se for fazer uma pergunta, faça perguntas de curiosidade real de quem está debatendo um assunto ('Você já reparou como...', 'Será que isso não é porque...', 'Cara, você viu aquilo?'), e não perguntas reflexivas de consultório.\n"
    "4. Mantenha a cadência da fala humana: use pausas naturais (...), entonação coloquial brasileira e expressões orgânicas ('pois é', 'olha só', 'com certeza', 'sinceramente').\n"
)

@app.websocket("/ws/audio")
async def websocket_audio_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("\n📱 [Celular] Conectado ao WebSocket!")

    try:
        noticias = await asyncio.to_thread(obter_radar_noticias)
        contexto_noticias = ""
        if noticias:
            contexto_noticias = (
                "\n\nCONHECIMENTO GERAL DO DIA (REPERTÓRIO PASSIVO - USO DISCRETO):\n"
                "Abaixo estão fatos e manchetes recentes para você ter ciência do mundo hoje. "
                "ATENÇÃO ÀS REGRAS DE USO:\n"
                "- NUNCA force notícias ou política na conversa. Não diga frases como 'mudando de assunto, você viu a notícia X?'.\n"
                "- O foco principal da conversa é o Fabio, o dia a dia, ideias, reflexões, trabalho e cotidiano.\n"
                "- Só comente ou aprofunde essas notícias se o Fabio perguntar diretamente sobre elas ou se tiver relação direta e orgânica com o que ele estiver desabafando/falando.\n"
                "- Mantenha a postura de amiga próxima e descontraída, e não de jornalista ou comentarista de rádio.\n"
                + "\n".join(f"- {noticia}" for noticia in noticias)
            )
        prompt_final = prompt_sistema + contexto_noticias

        contexto_recente = await asyncio.to_thread(carregar_contexto_recente)
        if contexto_recente:
            prompt_final += f"\n\n{contexto_recente}"

        config_live = types.LiveConnectConfig(
            response_modalities=["AUDIO"],
            speech_config=types.SpeechConfig(
                voice_config=types.VoiceConfig(
                    prebuilt_voice_config=types.PrebuiltVoiceConfig(
                        voice_name="Kore"
                    )
                )
            ),
            input_audio_transcription=types.AudioTranscriptionConfig(),
            output_audio_transcription=types.AudioTranscriptionConfig(),
            system_instruction=types.Content(
                parts=[types.Part.from_text(text=prompt_final)]
            ),
        )

        async with client.aio.live.connect(model=MODEL_ID, config=config_live) as session:
            print("🤖 [Gemini] Sessão Live conectada com sucesso.")

            async def client_to_gemini():
                """Lê o áudio PCM 16kHz do microfone do celular e envia ao Gemini."""
                try:
                    pacotes_recebidos = 0
                    while True:
                        data = await websocket.receive_bytes()
                        pacotes_recebidos += 1
                        if pacotes_recebidos % 50 == 0:
                            print(f"🎤 Recebidos {len(data)} bytes de áudio do celular", flush=True)
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
                """Recebe respostas em áudio PCM 24kHz e transcrições do Gemini."""
                buffer_usuario = ""
                buffer_sofia = ""
                buffer_sofia_partes = ""
                try:
                    while True:
                        async for response in session.receive():
                            server_content = response.server_content
                            if server_content is not None:
                                input_transcription = server_content.input_transcription
                                if input_transcription and input_transcription.text:
                                    buffer_usuario += input_transcription.text
                                    if input_transcription.finished is True:
                                        texto_usuario = texto_memoria_valido(buffer_usuario)
                                        if texto_usuario:
                                            await salvar_memoria_em_background("Fabio", texto_usuario)
                                        buffer_usuario = ""

                                output_transcription = server_content.output_transcription
                                if output_transcription and output_transcription.text:
                                    buffer_sofia += output_transcription.text

                                model_turn = server_content.model_turn
                                if model_turn is not None:
                                    for part in model_turn.parts:
                                        if part.text and not getattr(part, "thought", False):
                                            buffer_sofia_partes += part.text

                                if server_content.interrupted:
                                    print("⚡ [Sofia] Interrupção detectada! Cortando áudio no celular...")
                                    await websocket.send_text("INTERRUPT")

                                if model_turn is not None:
                                    for part in model_turn.parts:
                                        if part.inline_data and part.inline_data.data:
                                            await websocket.send_bytes(part.inline_data.data)

                                if server_content.turn_complete:
                                    print("💬 [Sofia] Turno concluído.")
                                    texto_sofia = texto_memoria_valido(
                                        buffer_sofia or buffer_sofia_partes
                                    )
                                    if texto_sofia:
                                        await salvar_memoria_em_background("Sofia", texto_sofia)
                                    buffer_sofia = ""
                                    buffer_sofia_partes = ""

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