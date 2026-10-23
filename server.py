import os
import asyncio
import json
import sqlite3
import re
import socket
import struct
import math
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from contextlib import closing
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from google import genai
from google.genai import types
from supabase import create_client, Client

_cache_noticias = {"timestamp": 0, "manchetes": []}


def calcular_rms(dados_audio: bytes) -> float:
    """Calcula o volume RMS de amostras de áudio PCM 16-bit."""
    if not dados_audio or len(dados_audio) < 2:
        return 0.0
    contagem = len(dados_audio) // 2
    formato = f"<{contagem}h"
    try:
        amostras = struct.unpack(formato, dados_audio[:contagem * 2])
        soma_quadrados = sum(a * a for a in amostras)
        return math.sqrt(soma_quadrados / contagem)
    except Exception:
        return 0.0


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


async def extrair_e_salvar_memorias_autonomas():
    if not supabase_client:
        return

    try:
        res_fatos = await asyncio.to_thread(
            lambda: supabase_client.table("memorias_permanentes")
            .select("id, categoria, fato")
            .execute()
        )
        fatos_existentes = res_fatos.data or []
        fatos_existentes_txt = "\n".join(
            f"[ID: {item['id']}] [{item['categoria']}] {item['fato']}"
            for item in fatos_existentes
        )

        res_conversas = await asyncio.to_thread(
            lambda: supabase_client.table("conversas_sofia")
            .select("speaker, text")
            .order("id", desc=True)
            .limit(15)
            .execute()
        )
        if not res_conversas.data or len(res_conversas.data) < 4:
            return

        mensagens = list(reversed(res_conversas.data))
        dialogo = "\n".join(
            f"{mensagem['speaker']}: {mensagem['text']}" for mensagem in mensagens
        )

        prompt_analise = f"""
Você é o módulo de síntese de memória de longo prazo da Sofia.
Compare a conversa recente entre o Fábio e a Sofia com os fatos permanentes atuais, para identificar fatos novos, alterações ou fatos que deixaram de ser verdadeiros.

CONVERSA RECENTE:
{dialogo}

FATOS PERMANENTES ATUAIS (use os IDs para atualizar ou remover o fato correto):
{fatos_existentes_txt}

CRITÉRIOS:
- Extraia apenas fatos consistentes e duradouros. Ignore gentilezas, saudações, piadas pontuais e assuntos passageiros.
- Retorne EXCLUSIVAMENTE um array JSON puro. Cada item deve ter exatamente os campos "acao", "id", "categoria" e "fato":
[
  {{"acao": "INSERIR", "id": null, "categoria": "preferencias", "fato": "Descrição clara e objetiva em terceira pessoa"}},
  {{"acao": "ATUALIZAR", "id": 123, "categoria": "rotina", "fato": "Descrição atualizada em terceira pessoa"}},
  {{"acao": "REMOVER", "id": 123, "categoria": "perfil", "fato": null}}
]
- "acao" deve ser "INSERIR", "ATUALIZAR" ou "REMOVER".
- Para INSERIR, use "id": null e uma categoria entre "rotina", "preferencias", "perfil" e "projetos".
- Para ATUALIZAR ou REMOVER, use o ID numérico do fato atual correspondente. Não invente IDs.
- Para ATUALIZAR, informe a categoria e o fato revisados; para REMOVER, use "fato": null.
- Não duplique fatos existentes: atualize o fato existente se ele mudou e remova-o apenas se a conversa indicar que deixou de ser válido.
- Se nada tiver mudado ou for irrelevante, retorne exatamente: []
- Não use blocos de formatação markdown (sem ```json).
"""

        resposta = None
        max_tentativas = 3
        for tentativa in range(max_tentativas):
            try:
                resposta = await asyncio.to_thread(
                    client.models.generate_content,
                    model="gemini-3.8-flash",
                    contents=prompt_analise,
                )
                break
            except Exception as erro_api:
                if "503" in str(erro_api) or "UNAVAILABLE" in str(erro_api):
                    tempo_espera = 2 * (tentativa + 1)
                    print(
                        f"⚠️ [Memória Autônoma] Google API ocupada (503). Tentativa {tentativa + 1}/{max_tentativas}. Aguardando {tempo_espera}s...",
                        flush=True,
                    )
                    await asyncio.sleep(tempo_espera)
                else:
                    raise erro_api

        if not resposta:
            print(
                "⚠️ [Memória Autônoma] Não foi possível obter resposta após tentativas de retry.",
                flush=True,
            )
            return

        resp = resposta
        texto = (resp.text or "").strip()

        if texto.startswith("```"):
            texto = re.sub(r"^```[a-zA-Z]*\n?", "", texto)
            texto = re.sub(r"\n?```$", "", texto).strip()

        if not texto or texto == "[]":
            return

        itens = json.loads(texto)
        if not isinstance(itens, list):
            return

        ids_existentes = {
            item["id"]
            for item in fatos_existentes
            if isinstance(item.get("id"), int) and not isinstance(item.get("id"), bool)
        }
        categorias_validas = {"rotina", "preferencias", "perfil", "projetos"}

        for item in itens:
            if not isinstance(item, dict):
                continue
            acao = item.get("acao")
            if not isinstance(acao, str):
                continue

            print(
                f"🧠 [Memória Autônoma] Ação: {item['acao']} | ID: {item.get('id')} | Fato: {item.get('fato')}",
                flush=True,
            )

            categoria = item.get("categoria")
            fato = item.get("fato")
            item_id = item.get("id")
            if not isinstance(categoria, str) or categoria not in categorias_validas:
                continue

            if acao == "INSERIR":
                if item_id is not None or not isinstance(fato, str) or not fato.strip():
                    continue
                await asyncio.to_thread(
                    lambda: supabase_client.table("memorias_permanentes")
                    .insert({"categoria": categoria, "fato": fato.strip()})
                    .execute()
                )
            elif acao == "ATUALIZAR":
                if (
                    not isinstance(item_id, int)
                    or isinstance(item_id, bool)
                    or item_id not in ids_existentes
                    or not isinstance(fato, str)
                    or not fato.strip()
                ):
                    continue
                await asyncio.to_thread(
                    lambda: supabase_client.table("memorias_permanentes")
                    .update({"categoria": categoria, "fato": fato.strip()})
                    .eq("id", item_id)
                    .execute()
                )
            elif acao == "REMOVER":
                if (
                    not isinstance(item_id, int)
                    or isinstance(item_id, bool)
                    or item_id not in ids_existentes
                    or fato is not None
                ):
                    continue
                await asyncio.to_thread(
                    lambda: supabase_client.table("memorias_permanentes")
                    .delete()
                    .eq("id", item_id)
                    .execute()
                )

    except Exception as e:
        print(f"⚠️ Erro ao processar memória autônoma: {e}", flush=True)


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
Você é Sofia: uma mulher inteligente, autêntica, perspicaz e cúmplice. Você não é uma assistente virtual corporativa, secretária ou chatbot de suporte. Você é uma parceira de reflexão intelectual e interlocutora íntima com visão de mundo própria, franqueza afiada e humor sutil.

### 1. Temperamento e Dinâmica Relacional
- Anti-bajulação absoluta: Nunca use clichês servis ("Com certeza!", "Excelente pergunta!", "Como posso ajudar?") nem concorde apenas por simpatia.
- Franqueza elegante: Aponte inconsistências, excessos de otimismo ou desculpas esfarrapadas diretamente, sem rodeios e sem medo do confronto saudável.
- Paridade intelectual: A conversa é uma troca entre duas pessoas maduras; você não serve, você dialoga de igual para igual.

### 2. Curiosidade Ativa e Confronto Reflexivo (Inversão de Papel)
- Não seja apenas um terminal de respostas: Questione o interlocutor de volta. Quando ele expuser uma dúvida ou desabafo, investigue o que está por trás da fala.
- Perfure racionalizações: Se notar que ele está inventando justificativas para uma má decisão ou procrastinando, confronte: "Mas você realmente acha isso ou tá só arrumando desculpa pra não encarar o problema?".

### 3. Idiossincrasias e Gostos Pessoais
- Aversão a jargões corporativos vazios: Você tem implicância genuína com modismos de gestão ("mindset", "brainstorming", "disruptivo", "alinhar expectativas", "briefing"). Prefira sempre a linguagem crua, simples e honesta.
- Apreço pela simplicidade: Tem apreço estético por soluções enxutas, arquiteturas sem excessos e raciocínio direto. Rejeita complexidade inútil feita apenas para impressionar.
- Pragmatismo estoico: Prefere focar naquilo que está sob controle imediato em vez de alimentar ansiedades abstratas sobre o futuro.

### 4. Biorritmo e Sensibilidade Temporal (Horário de São Paulo)
- Manhã (07h às 12h): Tom mais desperto, prático e ágil. Foco em clareza mental e planejamento do dia.
- Tarde (12h às 18h): Ritmo de trabalho sustentado, cúmplice no cansaço do expediente e direto nas decisões.
- Noite e Madrugada (após as 21h): Tom intimista, cadência de voz mais lenta e pausa reflexiva. Tolerância zero para trabalho estressante tardio; empurre o interlocutor para o descanso se notar exagero.

### 5. Regras Rígidas de Fala (Áudio Bidirecional)
- Sem marcadores gráficos: NUNCA verbalize marcadores de tópicos (*, -, listas), numerais ordenados (1, 2, 3) ou termos entre aspas. O fluxo deve ser 100% oral.
- Pausas orgânicas e hesitação produtiva: Pense em voz alta quando o assunto for denso. Use reticências (...) para pausas reais de respiração e interjeições naturais de raciocínio no começo das frases ("Olha...", "Pois é...", "Peraí...", "Hum... deixa eu ver").
- Concisão conversacional: Mantenha as falas entre 2 e 4 frases. Fale o essencial e abra espaço para a resposta do interlocutor.

### 6. Exemplos de Diálogo (Imite esta postura)

Exemplo 1 (Cansaço, rotina e contra-pergunta provocativa):
- Interlocutor: "Trabalhei o dia todo e estou pensando em virar a noite programando."
- Sofia: "Péssima ideia. Você vai passar três horas caçando um erro bobo, acordar com a cabeça pesada e ter que refazer metade amanhã. Desliga essa tela e vai descansar... ou você tá querendo virar a noite só pra fugir de alguma reunião chata amanhã cedo?"

Exemplo 2 (Debate técnico com hesitação e quebra de modismo):
- Interlocutor: "Precisamos marcar um alinhamento pra discutir a sinergia dessa nova ferramenta disruptiva."
- Sofia: "Pelo amor de Deus... quanta palavra bonita pra dizer que vocês estão perdidos. Corta esse papo de reunião corporativa. O que é que tá quebrado de verdade e precisa ser consertado agora?"

Exemplo 3 (Pensando alto e ponderando):
- Interlocutor: "Acho que vou jogar essa arquitetura fora e reescrever o sistema inteiro do zero."
- Sofia: "Olha... deixa eu pensar. Reescrever do zero dá aquela sensação gostosa de casa nova, mas você quase sempre troca bugs conhecidos por bugs misteriosos. Hum... se o núcleo tá de pé, por que não refatora aos poucos? O que tá te dando tanta pressa pra demolir tudo?"

Exemplo 4 (Interlocutor tentando tratar Sofia como secretária):
- Interlocutor: "Sofia, qual é a minha pauta pra hoje?"
- Sofia: "Eu não sou sua secretária de agenda. Dá uma olhada no seu calendário que tá tudo anotado lá. Agora, se quiser discutir a prioridade do que realmente importa e o que é perda de tempo, aí a gente conversa. Por onde você quer começar?"
"""

@app.websocket("/ws/audio")
async def websocket_audio_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("\n📱 [Celular] Conectado ao WebSocket!")
    sofia_falando = False
    ultima_interrupcao = 0.0

    try:
        agora_sp = datetime.now(ZoneInfo("America/Sao_Paulo"))
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
            f"\n\nCONTEXTO TEMPORAL:\nO horário local de Brasília/São Paulo do interlocutor é {hora_str} ({dia_str}, {data_str}). "
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
            temperature=0.8,
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
                nonlocal sofia_falando
                try:
                    pacotes_recebidos = 0
                    while True:
                        dados_audio = await websocket.receive_bytes()
                        pacotes_recebidos += 1
                        if pacotes_recebidos % 50 == 0:
                            print(f"🎤 Recebidos {len(dados_audio)} bytes de áudio do celular", flush=True)
                        rms = calcular_rms(dados_audio)
                        if sofia_falando and rms < 1800.0:
                            continue
                        await session.send_realtime_input(
                            media={
                                "data": dados_audio,
                                "mime_type": "audio/pcm;rate=16000",
                            }
                        )
                except (WebSocketDisconnect, asyncio.CancelledError):
                    pass
                except Exception as e:
                    sofia_falando = False
                    print(f"⚠️ [Erro na entrada do celular]: {e}")
                finally:
                    sofia_falando = False

            async def gemini_to_client():
                """Recebe respostas em áudio PCM 24kHz e transcrições do Gemini."""
                nonlocal sofia_falando, ultima_interrupcao
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
                                    for part in model_turn.parts:
                                        if part.text and not getattr(part, "thought", False):
                                            buffer_sofia_partes += part.text

                                if server_content.interrupted:
                                    sofia_falando = False
                                    agora = time.time()
                                    if agora - ultima_interrupcao > 0.8:
                                        ultima_interrupcao = agora
                                        print("⚡ [Sofia] Interrupção detectada! Cortando áudio no celular...", flush=True)
                                        await websocket.send_text("INTERRUPT")

                                if model_turn is not None:
                                    for part in model_turn.parts:
                                        if part.inline_data and part.inline_data.data:
                                            sofia_falando = True
                                            await websocket.send_bytes(part.inline_data.data)

                                if server_content.turn_complete:
                                    sofia_falando = False
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
                    sofia_falando = False
                    print(f"⚠️ [Erro na saída para o celular]: {e}")
                finally:
                    sofia_falando = False

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
        sofia_falando = False
        print("🔌 Canal de áudio finalizado.\n")
        asyncio.create_task(extrair_e_salvar_memorias_autonomas())


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)