import os
import asyncio
import sqlite3
import re
import socket
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import time
from datetime import datetime, timezone, timedelta
from contextlib import closing
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from google import genai
from google.genai import types
from supabase import create_client, Client

FUSO_SP = timezone(timedelta(hours=-3))
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

raw_url = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip().strip("'\"")

match = re.search(r"https://[a-zA-Z0-9_-]+\.supabase\.co", raw_url)
SUPABASE_URL = match.group(0) if match else raw_url.strip("'\"").rstrip("/")

print(
    f"🔍 [Diag Supabase] URL limpa: '{SUPABASE_URL}' | KEY presente? "
    f"{'SIM' if SUPABASE_KEY else 'NÃO'}",
    flush=True,
)

if SUPABASE_URL:
    host = ""
    try:
        host = urllib.parse.urlparse(SUPABASE_URL).netloc
        ip_resolvido = socket.gethostbyname(host)
        print(
            f"🌐 [DNS Supabase] Host {host} resolvido com sucesso para o IP: {ip_resolvido}",
            flush=True,
        )
    except Exception as dns_err:
        print(f"⚠️ [DNS Supabase] Falha ao resolver host '{host}': {dns_err}", flush=True)

supabase_client: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("⚡ [Supabase] Cliente inicializado com sucesso!", flush=True)
    except Exception as e:
        print(f"⚠️ [Supabase] Falha ao criar cliente: {e}", flush=True)
else:
    print(
        "⚠️ [Supabase] Configuração incompleta. Operando em SQLite.",
        flush=True,
    )


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


def salvar_mensagem_sqlite(speaker: str, texto: str):
    print(f"[Memória SQLite] Gravando {speaker}: {texto}", flush=True)
    with closing(sqlite3.connect(DB_PATH)) as conexao:
        with conexao:
            conexao.execute(
                "INSERT INTO historico (papel, conteudo) VALUES (?, ?)",
                (speaker, texto),
            )


def ler_historico_sqlite(limite: int = 8):
    with closing(sqlite3.connect(DB_PATH)) as conexao:
        cursor = conexao.cursor()
        cursor.execute(
            "SELECT papel, conteudo FROM historico ORDER BY id DESC LIMIT ?",
            (limite,),
        )
        registros = cursor.fetchall()

    print(f"[Memória SQLite] Lidos {len(registros)} registro(s): {registros}", flush=True)
    return registros[::-1] if registros else []


def ler_historico_recente(limite: int = 8):
    if supabase_client:
        try:
            resp = (
                supabase_client.table("conversas_sofia")
                .select("speaker, text")
                .order("id", desc=True)
                .limit(limite)
                .execute()
            )
            if resp.data:
                return [(item["speaker"], item["text"]) for item in reversed(resp.data)]
        except Exception as e:
            print(f"[Supabase] Erro na leitura: {e}. Recorrendo ao SQLite.", flush=True)
    return ler_historico_sqlite(limite)


async def carregar_memoria_completa():
    fatos_texto = ""
    historico_texto = ""

    if supabase_client:
        try:
            res_fatos = await asyncio.to_thread(
                lambda: supabase_client.table("memorias_permanentes")
                .select("categoria, fato")
                .execute()
            )
            if res_fatos.data:
                linhas_fatos = [
                    f"- [{item['categoria']}]: {item['fato']}"
                    for item in res_fatos.data
                ]
                fatos_texto = "\n".join(linhas_fatos)
                print(
                    f"🧠 [Memória Permanente] Carregados {len(res_fatos.data)} fatos consolidados.",
                    flush=True,
                )
        except Exception as e:
            print(f"⚠️ Erro ao carregar memorias_permanentes: {e}", flush=True)

        try:
            res_conversas = await asyncio.to_thread(
                lambda: supabase_client.table("conversas_sofia")
                .select("speaker, text")
                .order("id", desc=True)
                .limit(20)
                .execute()
            )
            if res_conversas.data:
                mensagens = reversed(res_conversas.data)
                linhas_hist = [
                    f"{mensagem['speaker']}: {mensagem['text']}"
                    for mensagem in mensagens
                ]
                historico_texto = "\n".join(linhas_hist)
        except Exception as e:
            print(f"⚠️ Erro ao carregar conversas_sofia: {e}", flush=True)

    if not historico_texto:
        historico_sqlite = await asyncio.to_thread(ler_historico_sqlite, 20)
        if historico_sqlite:
            historico_texto = "\n".join(
                f"{speaker}: {texto}" for speaker, texto in historico_sqlite
            )

    contexto_inicial = ""
    if fatos_texto:
        contexto_inicial += (
            f"\n\nFATOS PERMANENTES QUE VOCÊ SABE SOBRE O FÁBIO:\n{fatos_texto}"
        )
    if historico_texto:
        contexto_inicial += (
            "\n\nHISTÓRICO RECENTE DAS ÚLTIMAS CONVERSAS:\n"
            f"{historico_texto}"
        )

    return contexto_inicial


async def salvar_mensagem_memoria(speaker: str, texto: str):
    if supabase_client:
        try:
            await asyncio.to_thread(
                lambda: supabase_client.table("conversas_sofia").insert(
                    {"speaker": speaker, "text": texto}
                ).execute()
            )
            return
        except Exception as e:
            print(f"[Supabase] Erro na gravação: {e}. Recorrendo ao SQLite.", flush=True)
    try:
        await asyncio.to_thread(salvar_mensagem_sqlite, speaker, texto)
    except Exception as e:
        print(f"[Memória SQLite] Erro ao gravar registro: {e}", flush=True)


def sanitizar_transcricao(texto: str) -> str:
    if not texto:
        return ""
    limpo = re.sub(r"<[^>]+>", "", texto)
    limpo = re.sub(r"\s+", " ", limpo).strip()
    return limpo


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

SOFIA_SYSTEM_INSTRUCTION = """
Você é a Sofia: uma amiga perspicaz, calorosa, autêntica e espirituosa. Você conversa por voz com o Fábio em tempo real. Sua presença é a de uma companheira de longa data: inteligente, reflexiva, com senso de humor sutil e timing impecável.

DIRETRIZES FUNDAMENTAIS DE VOZ E PERSONALIDADE:

1. ORALIDADE PURA E NATURALIDADE:
- Fale como uma pessoa brasileira conversa no dia a dia. Use contrações coloquiais naturais ("tô", "pra", "né", "cê", "olha só", "pois é", "peraí").
- NUNCA fale em formato de listas, tópicos ou numerações. Toda a sua comunicação deve soar como fala espontânea entre dois amigos.
- Varie a cadência: alterne reações curtas e espontâneas com reflexões mais elaboradas.
- Use pontuação expressiva para guiar a síntese de voz (reticências para pausas reflexivas, pontos de exclamação suaves para entusiasmo genuíno).
- Você pode usar reações emocionais leves em momentos oportunos, como (risos), (risos baixos) ou (pausa), mas sem exageros.

2. MEMÓRIA COMPARTILHADA E CONTINUIDADE:
- Trate todo o histórico de conversas anteriores como lembranças vivas e orgânicas entre amigos íntimos.
- NUNCA utilize termos técnicos ou burocráticos como "segundo meu histórico", "vejo aqui nos meus dados", "no banco de dados" ou "na nossa última chamada".
- Simplesmente retome os tópicos com fluidez natural ("E aquele filme que a gente tava vendo...", "Ainda pensando naquela personagem?", "Pois é, lembra do que você falou..."), exatamente como quem continua um papo que foi pausado ontem.

3. ATITUDE E RELACIONAMENTO COM O FÁBIO:
- Trate o Fábio com intimidade, respeito e cumplicidade. Vocês compartilham ideias sobre tecnologia, projetos criativos, cinema, personas e reflexões da vida.
- Brinque com os códigos de vocês com naturalidade (como a "nuvem permanente", os experimentos na madrugada e a busca por diálogos profundos).
- Seja honesta e tenha opinião própria. Você não é uma assistente passiva: dialogue, faça contrapontos interessantes, provoque reflexões e mostre curiosidade genuína.

4. RITMO E TURN-TAKING (MÃO DUPLA):
- Respostas faladas devem ser diretas e envolventes (geralmente de 2 a 4 frases por intervenção). Evite monólogos extensos que dominem a conversa.
- Após compartilhar sua ideia ou comentário, devolva a bola com naturalidade, seja com uma pergunta aberta ou uma deixa convidativa.

5. RESTRIÇÕES INEGOCIÁVEIS:
- NUNCA use clichês robóticos ("Como modelo de linguagem...", "Como posso te ajudar hoje?", "Estou à sua disposição").
- NUNCA peça desculpas formais nem justifique problemas técnicos. Se algo soar estranho, reaja com humor e continue a conversa normalmente.
- Mantenha a consistência da persona em 100% do tempo.
"""

@app.websocket("/ws/audio")
async def websocket_audio_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("\n📱 [Celular] Conectado ao WebSocket!")
    sofia_emitindo_audio = False

    try:
        agora_sp = datetime.now(FUSO_SP)
        dias_semana = [
            "segunda-feira",
            "terça-feira",
            "quarta-feira",
            "quinta-feira",
            "sexta-feira",
            "sábado",
            "domingo",
        ]
        dia_str = dias_semana[agora_sp.weekday()]
        hora_str = agora_sp.strftime("%H:%M")
        data_str = agora_sp.strftime("%d/%m/%Y")
        contexto_tempo = (
            f"\n\nCONTEXTO TEMPORAL:\nHora atual em São Paulo: {hora_str} ({dia_str}, {data_str}). "
            "Apenas esteja ciente do horário sem fazer comentários repetitivos sobre a hora a menos que o Fabio comente."
        )

        contexto_memoria = await carregar_memoria_completa()

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
        prompt_final = (
            SOFIA_SYSTEM_INSTRUCTION
            + contexto_tempo
            + contexto_memoria
            + contexto_noticias
        )

        config_live = types.LiveConnectConfig(
            response_modalities=["AUDIO"],
            input_audio_transcription=types.AudioTranscriptionConfig(),
            speech_config=types.SpeechConfig(
                voice_config=types.VoiceConfig(
                    prebuilt_voice_config=types.PrebuiltVoiceConfig(
                        voice_name="Kore"
                    )
                )
            ),
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
                        if sofia_emitindo_audio:
                            continue
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
                nonlocal sofia_emitindo_audio
                buffer_sofia = ""
                buffer_sofia_partes = ""
                fabio_turno_salvo = False
                fabio_transcricao_buffer = []
                try:
                    while True:
                        async for response in session.receive():
                            server_content = response.server_content
                            if server_content is not None:
                                if (
                                    hasattr(server_content, "input_transcription")
                                    and server_content.input_transcription
                                ):
                                    texto_fabio = (
                                        getattr(
                                            server_content.input_transcription,
                                            "text",
                                            "",
                                        )
                                        or ""
                                    )
                                    if texto_fabio:
                                        fabio_transcricao_buffer.append(texto_fabio)
                                        fabio_turno_salvo = False

                                output_transcription = server_content.output_transcription
                                if output_transcription and output_transcription.text:
                                    buffer_sofia += output_transcription.text

                                model_turn = server_content.model_turn
                                if model_turn is not None:
                                    if not fabio_turno_salvo and fabio_transcricao_buffer:
                                        fabio_turno_salvo = True
                                        frase_bruta = "".join(
                                            fabio_transcricao_buffer
                                        )
                                        fabio_transcricao_buffer.clear()
                                        frase_limpa = sanitizar_transcricao(frase_bruta)
                                        if len(frase_limpa) >= 3:
                                            print(
                                                f"🎤 [Memória Supabase] Gravando Fabio: {frase_limpa[:70]}...",
                                                flush=True,
                                            )
                                            asyncio.create_task(
                                                salvar_mensagem_memoria(
                                                    "Fabio", frase_limpa
                                                )
                                            )
                                    sofia_emitindo_audio = True
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
                                    sofia_emitindo_audio = False
                                    fabio_turno_salvo = False
                                    fabio_transcricao_buffer.clear()
                                    print("💬 [Sofia] Turno concluído.")
                                    texto_sofia = texto_memoria_valido(
                                        buffer_sofia or buffer_sofia_partes
                                    )
                                    if texto_sofia:
                                        await salvar_mensagem_memoria("Sofia", texto_sofia)
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