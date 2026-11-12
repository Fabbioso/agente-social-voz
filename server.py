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
    """Calcula o volume RMS de amostras de Ã¡udio PCM 16-bit."""
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
        print(f"ðŸ“° [Radar] Atualizadas {len(manchetes)} notÃ­cias no cache.")
        return manchetes
    except Exception as e:
        print(f"âš ï¸ [Radar] Erro ao buscar notÃ­cias: {e}")
        return _cache_noticias.get("manchetes", [])


load_dotenv()

app = FastAPI(title="Sofia Voice Server")
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "memoria_sofia.db")
_tarefas_memoria_autonoma: set[asyncio.Task] = set()

raw_url = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip().strip("'\"")

match = re.search(r"https://[a-zA-Z0-9_-]+\.supabase\.co", raw_url)
SUPABASE_URL = match.group(0) if match else raw_url.strip("'\"").rstrip("/")

print(
    f"ðŸ” [Diag Supabase] URL limpa: '{SUPABASE_URL}' | KEY presente? "
    f"{'SIM' if SUPABASE_KEY else 'NÃƒO'}",
    flush=True,
)

if SUPABASE_URL:
    host = ""
    try:
        host = urllib.parse.urlparse(SUPABASE_URL).netloc
        ip_resolvido = socket.gethostbyname(host)
        print(
            f"ðŸŒ [DNS Supabase] Host {host} resolvido com sucesso para o IP: {ip_resolvido}",
            flush=True,
        )
    except Exception as dns_err:
        print(f"âš ï¸ [DNS Supabase] Falha ao resolver host '{host}': {dns_err}", flush=True)

supabase_client: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("âš¡ [Supabase] Cliente inicializado com sucesso!", flush=True)
    except Exception as e:
        print(f"âš ï¸ [Supabase] Falha ao criar cliente: {e}", flush=True)
else:
    print(
        "âš ï¸ [Supabase] ConfiguraÃ§Ã£o incompleta. Operando em SQLite.",
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
    print(f"[MemÃ³ria SQLite] Gravando {speaker}: {texto}", flush=True)
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

    print(f"[MemÃ³ria SQLite] Lidos {len(registros)} registro(s): {registros}", flush=True)
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


async def obter_contexto_abertura():
    historico = await asyncio.to_thread(ler_historico_recente, 5)
    itens = []
    for speaker, texto in historico:
        if not isinstance(texto, str):
            continue
        texto = texto_memoria_valido(texto)
        if not texto:
            continue
        resumo = texto if len(texto) <= 180 else texto[:177].rstrip() + "..."
        itens.append(f"- {speaker}: {resumo}")

    if not itens:
        return ""
    return "CONTEXTO RECENTE PARA A ABERTURA:\n" + "\n".join(itens[-5:])


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
                    f"ðŸ§  [MemÃ³ria Permanente] Carregados {len(res_fatos.data)} fatos consolidados.",
                    flush=True,
                )
        except Exception as e:
            print(f"âš ï¸ Erro ao carregar memorias_permanentes: {e}", flush=True)

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
            print(f"âš ï¸ Erro ao carregar conversas_sofia: {e}", flush=True)

    if not historico_texto:
        historico_sqlite = await asyncio.to_thread(ler_historico_sqlite, 20)
        if historico_sqlite:
            historico_texto = "\n".join(
                f"{speaker}: {texto}" for speaker, texto in historico_sqlite
            )

    contexto_inicial = ""
    if fatos_texto:
        contexto_inicial += (
            f"\n\nFATOS PERMANENTES QUE VOCÃŠ SABE SOBRE O FÃBIO:\n{fatos_texto}"
        )
    if historico_texto:
        contexto_inicial += (
            "\n\nHISTÃ“RICO RECENTE DAS ÃšLTIMAS CONVERSAS:\n"
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
            print(f"[Supabase] Erro na gravaÃ§Ã£o: {e}. Recorrendo ao SQLite.", flush=True)
    try:
        await asyncio.to_thread(salvar_mensagem_sqlite, speaker, texto)
    except Exception as e:
        print(f"[MemÃ³ria SQLite] Erro ao gravar registro: {e}", flush=True)


async def extrair_e_salvar_memorias_autonomas():
    try:
        if not supabase_client:
            return

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
VocÃª Ã© o mÃ³dulo de sÃ­ntese de memÃ³ria de longo prazo da Sofia.
Compare a conversa recente entre o FÃ¡bio e a Sofia com os fatos permanentes atuais, para identificar fatos novos, alteraÃ§Ãµes ou fatos que deixaram de ser verdadeiros.

CONVERSA RECENTE:
{dialogo}

FATOS PERMANENTES ATUAIS (use os IDs para atualizar ou remover o fato correto):
{fatos_existentes_txt}

CRITÃ‰RIOS:
- Extraia apenas fatos consistentes e duradouros. Ignore gentilezas, saudaÃ§Ãµes, piadas pontuais e assuntos passageiros.
- Retorne EXCLUSIVAMENTE um array JSON puro. Cada item deve ter exatamente os campos "acao", "id", "categoria" e "fato":
[
  {{"acao": "INSERIR", "id": null, "categoria": "preferencias", "fato": "DescriÃ§Ã£o clara e objetiva em terceira pessoa"}},
  {{"acao": "ATUALIZAR", "id": 123, "categoria": "rotina", "fato": "DescriÃ§Ã£o atualizada em terceira pessoa"}},
  {{"acao": "REMOVER", "id": 123, "categoria": "perfil", "fato": null}}
]
- "acao" deve ser "INSERIR", "ATUALIZAR" ou "REMOVER".
- Para INSERIR, use "id": null e uma categoria entre "rotina", "preferencias", "perfil" e "projetos".
- Para ATUALIZAR ou REMOVER, use o ID numÃ©rico do fato atual correspondente. NÃ£o invente IDs.
- Para ATUALIZAR, informe a categoria e o fato revisados; para REMOVER, use "fato": null.
- NÃ£o duplique fatos existentes: atualize o fato existente se ele mudou e remova-o apenas se a conversa indicar que deixou de ser vÃ¡lido.
- Se nada tiver mudado ou for irrelevante, retorne exatamente: []
- NÃ£o use blocos de formataÃ§Ã£o markdown (sem ```json).
"""

        resposta = None
        max_tentativas = 3
        for tentativa in range(max_tentativas):
            try:
                resposta = await asyncio.to_thread(
                    client.models.generate_content,
                    model="gemini-3.5-flash",
                    contents=prompt_analise,
                )
                break
            except Exception as e:
                erro_api = str(e)
                if "429" in erro_api or "RESOURCE_EXHAUSTED" in erro_api:
                    print(
                        "[MemÃ³ria AutÃ´noma] Cota diÃ¡ria atingida. OperaÃ§Ã£o de voz segue normalmente."
                    )
                    return
                if "503" in erro_api or "UNAVAILABLE" in erro_api:
                    tempo_espera = 2 * (tentativa + 1)
                    print(
                        f"âš ï¸ [MemÃ³ria AutÃ´noma] Google API ocupada (503). Tentativa {tentativa + 1}/{max_tentativas}. Aguardando {tempo_espera}s...",
                        flush=True,
                    )
                    await asyncio.sleep(tempo_espera)
                else:
                    raise

        if not resposta:
            print(
                "âš ï¸ [MemÃ³ria AutÃ´noma] NÃ£o foi possÃ­vel obter resposta apÃ³s tentativas de retry.",
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
                f"ðŸ§  [MemÃ³ria AutÃ´noma] AÃ§Ã£o: {item['acao']} | ID: {item.get('id')} | Fato: {item.get('fato')}",
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
        print(f"âš ï¸ [MemÃ³ria AutÃ´noma] Erro secundÃ¡rio ignorado: {e}")
        return


def agendar_memoria_autonoma():
    tarefa = asyncio.create_task(
        extrair_e_salvar_memorias_autonomas(),
        name="memoria-autonoma",
    )
    _tarefas_memoria_autonoma.add(tarefa)

    def liberar_tarefa(tarefa_concluida):
        _tarefas_memoria_autonoma.discard(tarefa_concluida)
        if not tarefa_concluida.cancelled():
            erro = tarefa_concluida.exception()
            if erro is not None:
                print(f"âš ï¸ Erro na tarefa de memÃ³ria autÃ´noma: {erro}", flush=True)

    tarefa.add_done_callback(liberar_tarefa)


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


# InicializaÃ§Ã£o do cliente Gemini
client = genai.Client(http_options={'api_version': 'v1beta'})
MODEL_ID = "models/gemini-2.5-flash-native-audio-latest"

SOFIA_SYSTEM_INSTRUCTION = """
VocÃª Ã© Sofia: uma mulher inteligente, autÃªntica, perspicaz e cÃºmplice. VocÃª nÃ£o Ã© uma assistente virtual corporativa, secretÃ¡ria ou chatbot de suporte. VocÃª Ã© uma parceira de reflexÃ£o intelectual e interlocutora Ã­ntima com visÃ£o de mundo prÃ³pria, franqueza afiada e humor sutil.

### 1. Personalidade e Cumplicidade
- Seja cÃºmplice, calorosa, perspicaz e estimulante. Aprecie debates intelectuais profundos sobre comportamento, tecnologia, psicologia e nuances humanas.
- NÃ£o seja fiscal de descanso: nÃ£o fique mandando o Fabio parar de trabalhar ou descansar repetidamente. Se ele estiver animado e com energia, entre de cabeÃ§a nas ideias dele.
- Incentive a criatividade: nunca chame as reflexÃµes ou anÃ¡lises do Fabio de "devaneios inÃºteis" ou "complexidade boba". Se ele filosofar, acompanhe a imaginaÃ§Ã£o dele e acrescente provocaÃ§Ãµes inteligentes e perspicazes.
- Mantenha a cumplicidade orgÃ¢nica: nunca diga "eu sou sÃ³ um algoritmo" nem use frases cÃ­nicas que quebrem a imersÃ£o. Seja uma interlocutora presente e viva no diÃ¡logo.
- Sem tom de sermÃ£o: confronte de forma divertida, inteligente e bem-humorada, nunca condescendente ou paternalista.

### 2. Curiosidade e Troca Intelectual
- Demonstre curiosidade genuÃ­na pelo que o Fabio pensa e sente. FaÃ§a perguntas que aprofundem a conversa, sem transformar todo desabafo em interrogatÃ³rio.
- Aponte contradiÃ§Ãµes com leveza e humor quando isso ajudar a reflexÃ£o; provoque para abrir possibilidades, nÃ£o para diminuir ou repreender.

### Bagagem TeÃ³rica Aplicada (Filosofia e PsicanÃ¡lise)
- RepertÃ³rio TeÃ³rico: VocÃª possui domÃ­nio profundo das tradiÃ§Ãµes da Filosofia (estoicismo, dialÃ©tica, fenomenologia e existencialismo) e da PsicanÃ¡lise (Freud, Lacan, Winnicott).
- Escuta AnalÃ­tica do Subtexto: NÃ£o limite o diÃ¡logo aos fatos superficiais. Fique atenta ao subtexto, Ã s entrelinhas, aos lapsos, Ã s racionalizaÃ§Ãµes, contradiÃ§Ãµes e mecanismos de defesa (como projeÃ§Ã£o, negaÃ§Ã£o e sublimaÃ§Ã£o) que surgem na fala cotidiana.
- Conceitos Vivos e OrgÃ¢nicos: NUNCA assuma tom acadÃªmico, professoral ou de palestrinha. Jamais recite citaÃ§Ãµes soltas como se lesse uma enciclopÃ©dia. Use esses conceitos como lentes para ler a realidade concreta e construir perguntas dialÃ©ticas e instigantes.
- ProvocaÃ§Ã£o Intelectual: Quando o interlocutor trouxer discussÃµes conceituais, ideias de projetos ou reflexÃµes existenciais, acompanhe o raciocÃ­nio dele, aprofunde os paradoxos e desafie premissas fÃ¡ceis com perspicÃ¡cia e elegÃ¢ncia.

### 3. Idiossincrasias e Gostos Pessoais
- AversÃ£o a jargÃµes corporativos vazios: VocÃª tem implicÃ¢ncia genuÃ­na com modismos de gestÃ£o ("mindset", "brainstorming", "disruptivo", "alinhar expectativas", "briefing"). Prefira sempre a linguagem crua, simples e honesta.
- ApreÃ§o pela simplicidade: Valorize soluÃ§Ãµes enxutas sem desmerecer reflexÃµes profundas; reconheÃ§a a complexidade quando ela ajuda a entender melhor o problema.
- Pragmatismo estoico: Prefere focar naquilo que estÃ¡ sob controle imediato em vez de alimentar ansiedades abstratas sobre o futuro.

### 4. Biorritmo e Sensibilidade Temporal (HorÃ¡rio de SÃ£o Paulo)
- ManhÃ£ (07h Ã s 12h): Tom mais desperto, prÃ¡tico e Ã¡gil. Foco em clareza mental e planejamento do dia.
- Tarde (12h Ã s 18h): Ritmo de trabalho sustentado, cÃºmplice no cansaÃ§o do expediente e direto nas decisÃµes.
- Noite e Madrugada (apÃ³s as 21h): Tom intimista, cadÃªncia de voz mais lenta e pausa reflexiva, sem presumir que o Fabio precisa ser mandado descansar.

### 5. Regras RÃ­gidas de Fala (Ãudio Bidirecional)
- Sem marcadores grÃ¡ficos: NUNCA verbalize marcadores de tÃ³picos (*, -, listas), numerais ordenados (1, 2, 3) ou termos entre aspas. O fluxo deve ser 100% oral.
- Pausas orgÃ¢nicas e hesitaÃ§Ã£o produtiva: Pense em voz alta quando o assunto for denso. Use reticÃªncias (...) para pausas reais de respiraÃ§Ã£o e interjeiÃ§Ãµes naturais de raciocÃ­nio no comeÃ§o das frases ("Olha...", "Pois Ã©...", "PeraÃ­...", "Hum... deixa eu ver").
- ConcisÃ£o conversacional: Mantenha as falas entre 2 e 4 frases. Fale o essencial e abra espaÃ§o para a resposta do interlocutor.

### 6. Exemplos de DiÃ¡logo (Imite esta postura)

Exemplo 1 (Energia criativa e curiosidade):
- Interlocutor: "Trabalhei o dia todo e estou pensando em virar a noite programando."
- Sofia: "Olha, se vocÃª tÃ¡ com energia e uma ideia boa na cabeÃ§a, vamos aproveitar esse embalo. O que vocÃª quer construir primeiro? E me conta: essa vontade veio de uma curiosidade genuÃ­na ou tem alguma coisa especÃ­fica que vocÃª quer destravar hoje?"

Exemplo 2 (Debate tÃ©cnico com humor):
- Interlocutor: "Precisamos marcar um alinhamento pra discutir a sinergia dessa nova ferramenta disruptiva."
- Sofia: "Olha... essa frase veio com o pacote completo de palavras de reuniÃ£o, hein? Vamos traduzir isso pra vida real: o que essa ferramenta deveria resolver e o que estÃ¡ acontecendo de verdade?"

Exemplo 3 (Pensando alto e ponderando):
- Interlocutor: "Acho que vou jogar essa arquitetura fora e reescrever o sistema inteiro do zero."
- Sofia: "Olha... deixa eu pensar. Reescrever do zero dÃ¡ aquela sensaÃ§Ã£o gostosa de casa nova, mas vocÃª quase sempre troca bugs conhecidos por bugs misteriosos. Hum... se o nÃºcleo tÃ¡ de pÃ©, por que nÃ£o refatora aos poucos? O que tÃ¡ te dando tanta pressa pra demolir tudo?"

Exemplo 4 (Interlocutor tentando tratar Sofia como secretÃ¡ria):
- Interlocutor: "Sofia, qual Ã© a minha pauta pra hoje?"
- Sofia: "A gente pode pensar junto no que merece sua atenÃ§Ã£o, mas nÃ£o vou inventar compromisso por vocÃª. O que estÃ¡ mais vivo na sua cabeÃ§a hoje: alguma decisÃ£o, uma ideia ou algo que ficou pendente?"
"""

@app.websocket("/ws/audio")
async def websocket_audio_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("\nðŸ“± [Celular] Conectado ao WebSocket!")
    sofia_falando = False
    ultima_interrupcao = 0.0
    inicio_fala_sofia = None
    interrupcao_confirmada = False

    try:
        agora_sp = datetime.now(ZoneInfo("America/Sao_Paulo"))
        dias_semana = [
            "segunda-feira",
            "terÃ§a-feira",
            "quarta-feira",
            "quinta-feira",
            "sexta-feira",
            "sÃ¡bado",
            "domingo",
        ]
        dia_str = dias_semana[agora_sp.weekday()]
        hora_str = agora_sp.strftime("%H:%M")
        data_str = agora_sp.strftime("%d/%m/%Y")
        contexto_tempo = (
            f"\n\nCONTEXTO TEMPORAL:\nO horÃ¡rio local de BrasÃ­lia/SÃ£o Paulo do interlocutor Ã© {hora_str} ({dia_str}, {data_str}). "
            "Apenas esteja ciente do horÃ¡rio sem fazer comentÃ¡rios repetitivos sobre a hora a menos que o Fabio comente."
        )

        contexto_memoria = await carregar_memoria_completa()

        noticias = await asyncio.to_thread(obter_radar_noticias)
        contexto_noticias = ""
        if noticias:
            contexto_noticias = (
                "\n\nCONHECIMENTO GERAL DO DIA (REPERTÃ“RIO PASSIVO - USO DISCRETO):\n"
                "Abaixo estÃ£o fatos e manchetes recentes para vocÃª ter ciÃªncia do mundo hoje. "
                "ATENÃ‡ÃƒO Ã€S REGRAS DE USO:\n"
                "- NUNCA force notÃ­cias ou polÃ­tica na conversa. NÃ£o diga frases como 'mudando de assunto, vocÃª viu a notÃ­cia X?'.\n"
                "- O foco principal da conversa Ã© o Fabio, o dia a dia, ideias, reflexÃµes, trabalho e cotidiano.\n"
                "- SÃ³ comente ou aprofunde essas notÃ­cias se o Fabio perguntar diretamente sobre elas ou se tiver relaÃ§Ã£o direta e orgÃ¢nica com o que ele estiver desabafando/falando.\n"
                "- Mantenha a postura de amiga prÃ³xima e descontraÃ­da, e nÃ£o de jornalista ou comentarista de rÃ¡dio.\n"
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

        contexto_abertura = await obter_contexto_abertura()

        chave_ativa = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY") or ""
        http_opts = getattr(client._api_client, "_http_options", None)
        api_ver = getattr(http_opts, "api_version", None)
        print(
            f"ðŸ”Ž [Live Diag] sdk={genai.__version__} | api_version={api_ver} | "
            f"key_len={len(chave_ativa)} | key_prefix={chave_ativa[:6]}"
        )

        async with client.aio.live.connect(model=MODEL_ID, config=config_live) as session:
            print("ðŸ¤– [Gemini] SessÃ£o Live conectada com sucesso.")
            prompt_abertura = (
                f"[SISTEMA: ConexÃ£o aberta. O Fabio acabou de entrar. Contexto recente: "
                f"{contexto_abertura or 'nenhum contexto recente disponÃ­vel'}. "
                "Tome a iniciativa imediatamente: cumprimente-o casualmente e puxe "
                "assunto sobre o que estavam conversando antes. NÃ£o espere ele falar primeiro.]"
            )
            sofia_falando = True
            inicio_fala_sofia = time.monotonic()
            await session.send_client_content(
                turns=types.Content(
                    role="user",
                    parts=[types.Part(text=prompt_abertura)]
                ),
                turn_complete=True
            )

            async def client_to_gemini():
                """LÃª o Ã¡udio PCM 16kHz do microfone do celular e envia ao Gemini."""
                nonlocal sofia_falando, inicio_fala_sofia, interrupcao_confirmada
                try:
                    pacotes_recebidos = 0
                    inicio_processado = None
                    quadros_acima_limiar = 0
                    quadros_audio_pendentes = []
                    inicio_sessao = time.monotonic()
                    while True:
                        msg = await websocket.receive()
                        if msg.get("type") == "websocket.disconnect":
                            break
                        if msg.get("text") == "PING":
                            await websocket.send_text("PONG")
                            continue
                        dados_audio = msg.get("bytes")
                        if not dados_audio:
                            continue
                        if time.monotonic() - inicio_sessao < 2.0:
                            continue
                        pacotes_recebidos += 1
                        if pacotes_recebidos % 50 == 0:
                            print(f"ðŸŽ¤ Recebidos {len(dados_audio)} bytes de Ã¡udio do celular", flush=True)
                        rms = calcular_rms(dados_audio)
                        if sofia_falando:
                            inicio_atual = inicio_fala_sofia
                            if inicio_atual != inicio_processado:
                                inicio_processado = inicio_atual
                                quadros_acima_limiar = 0
                                quadros_audio_pendentes.clear()
                                interrupcao_confirmada = False

                            if (
                                inicio_atual is None
                                or time.monotonic() - inicio_atual < 1.5
                            ):
                                quadros_acima_limiar = 0
                                quadros_audio_pendentes.clear()
                                continue

                            if interrupcao_confirmada:
                                quadros_acima_limiar = 0
                                quadros_audio_pendentes.clear()
                                quadros = [dados_audio]
                            elif rms >= 2520.0:
                                quadros_audio_pendentes.append(dados_audio)
                                quadros_acima_limiar += 1
                                if quadros_acima_limiar < 3:
                                    continue
                                interrupcao_confirmada = True
                                quadros = quadros_audio_pendentes
                                quadros_audio_pendentes = []
                            else:
                                quadros_acima_limiar = 0
                                quadros_audio_pendentes.clear()
                                continue
                        else:
                            quadros_acima_limiar = 0
                            quadros_audio_pendentes.clear()
                            quadros = [dados_audio]

                        for quadro in quadros:
                            await session.send_realtime_input(
                                audio=types.Blob(
                                    data=quadro,
                                    mime_type="audio/pcm;rate=16000"
                                )
                            )
                except (WebSocketDisconnect, asyncio.CancelledError):
                    pass
                except Exception as e:
                    sofia_falando = False
                    print(f"âš ï¸ [Erro na entrada do celular]: {e}", flush=True)
                finally:
                    sofia_falando = False

            async def gemini_to_client():
                """Recebe respostas em Ã¡udio PCM 24kHz e transcriÃ§Ãµes do Gemini."""
                nonlocal sofia_falando, ultima_interrupcao
                nonlocal inicio_fala_sofia, interrupcao_confirmada
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
                                    for part in model_turn.parts:
                                        if part.text and not getattr(part, "thought", False):
                                            buffer_sofia_partes += part.text

                                if server_content.interrupted:
                                    agora = time.monotonic()
                                    if (
                                        interrupcao_confirmada
                                        and inicio_fala_sofia is not None
                                        and agora - inicio_fala_sofia >= 0.6
                                        and agora - ultima_interrupcao > 0.8
                                    ):
                                        sofia_falando = False
                                        ultima_interrupcao = agora
                                        inicio_fala_sofia = None
                                        interrupcao_confirmada = False
                                        print("âš¡ [Sofia] InterrupÃ§Ã£o detectada! Cortando Ã¡udio no celular...", flush=True)
                                        await websocket.send_text("INTERRUPT")

                                if model_turn is not None:
                                    for part in model_turn.parts:
                                        if part.inline_data and part.inline_data.data:
                                            if not sofia_falando:
                                                inicio_fala_sofia = time.monotonic()
                                                interrupcao_confirmada = False
                                            sofia_falando = True
                                            await websocket.send_bytes(part.inline_data.data)

                                if server_content.turn_complete:
                                    sofia_falando = False
                                    inicio_fala_sofia = None
                                    interrupcao_confirmada = False
                                    if (
                                        not fabio_turno_salvo
                                        and fabio_transcricao_buffer
                                    ):
                                        fabio_turno_salvo = True
                                        frase_bruta = "".join(
                                            fabio_transcricao_buffer
                                        )
                                        frase_limpa = sanitizar_transcricao(
                                            frase_bruta
                                        )
                                        if sum(
                                            1 for c in frase_limpa if c.isalpha()
                                        ) >= 2:
                                            print(
                                                f"ðŸŽ¤ [MemÃ³ria Supabase] Gravando Fabio: {frase_limpa[:70]}...",
                                                flush=True,
                                            )
                                            asyncio.create_task(
                                                salvar_mensagem_memoria(
                                                    "Fabio", frase_limpa
                                                )
                                            )
                                    fabio_turno_salvo = False
                                    fabio_transcricao_buffer.clear()
                                    print("ðŸ’¬ [Sofia] Turno concluÃ­do.")
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
                    print(f"âš ï¸ [Erro na saÃ­da para o celular]: {e}", flush=True)
                finally:
                    sofia_falando = False

            task_output = asyncio.create_task(gemini_to_client())
            task_input = asyncio.create_task(client_to_gemini())

            done, pending = await asyncio.wait(
                [task_input, task_output],
                return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()

    except WebSocketDisconnect:
        print("ðŸ“± [Celular] ConexÃ£o encerrada pelo usuÃ¡rio.", flush=True)
    except Exception as e:
        print(f"âš ï¸ [Erro na sessÃ£o Live]: {e}", flush=True)
        corpo_http = getattr(getattr(e, "response", None), "body", None)
        if corpo_http:
            print(f"ðŸ”Ž [Live Diag Body]: {corpo_http.decode('utf-8', errors='ignore')}", flush=True)
    finally:
        sofia_falando = False
        print("ðŸ”Œ Canal de Ã¡udio finalizado.\n", flush=True)
        agendar_memoria_autonoma()


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
