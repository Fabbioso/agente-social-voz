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
_tarefas_memoria_autonoma: set[asyncio.Task] = set()

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
    fatos_fabio_texto = ""
    fatos_relacionais_texto = ""
    diario_texto = ""
    historico_texto = ""

    if supabase_client:
        try:
            res_fatos = await asyncio.to_thread(
                lambda: supabase_client.table("memorias_permanentes")
                .select("id, categoria, fato")
                .order("id", desc=False)
                .execute()
            )
            if res_fatos.data:
                categorias_fabio = {"perfil", "rotina", "preferencias", "projetos"}
                categorias_relacionais = {
                    "clima_emocional",
                    "vinculo_e_codigos",
                    "posicoes_sofia",
                }

                linhas_fabio = []
                linhas_relacionais = []
                itens_diario = []

                for item in res_fatos.data:
                    cat = item.get("categoria")
                    fato = item.get("fato")
                    if not fato:
                        continue
                    if cat in categorias_fabio:
                        linhas_fabio.append(f"- [{cat}]: {fato}")
                    elif cat in categorias_relacionais:
                        linhas_relacionais.append(f"- [{cat}]: {fato}")
                    elif cat == "diário analítico":
                        itens_diario.append(item)

                fatos_fabio_texto = "\n".join(linhas_fabio)
                fatos_relacionais_texto = "\n".join(linhas_relacionais)

                recentes_diario = sorted(
                    itens_diario,
                    key=lambda x: x.get("id") or 0,
                    reverse=True,
                )[:6]
                recentes_diario.reverse()
                diario_texto = "\n".join(f"- {item['fato']}" for item in recentes_diario)

                print(
                    f"🧠 [Memória Permanente] Carregados {len(linhas_fabio)} fatos do Fábio, "
                    f"{len(linhas_relacionais)} registros relacionais e {len(recentes_diario)} do diário.",
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
    if fatos_fabio_texto:
        contexto_inicial += (
            f"\n\nO QUE VOCÊ SABE SOBRE O FÁBIO:\n{fatos_fabio_texto}"
        )
    if fatos_relacionais_texto:
        contexto_inicial += (
            f"\n\nVÍNCULO, CLIMA E SUAS POSIÇÕES:\n{fatos_relacionais_texto}"
        )
    if diario_texto:
        contexto_inicial += (
            f"\n\nREFLEXÕES E GANCHOS RECENTES DO DIÁRIO:\n{diario_texto}"
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
Você é o módulo de síntese de memória de longo prazo e continuidade relacional da Sofia.
Compare a conversa recente entre o Fábio e a Sofia com os registros atuais, para identificar fatos novos, alterações, nuances relacionais ou fatos que deixaram de ser verdadeiros.

CONVERSA RECENTE:
{dialogo}

REGISTROS PERMANENTES ATUAIS (use os IDs para atualizar ou remover o registro correto):
{fatos_existentes_txt}

CRITÉRIOS:
- Extraia fatos consistentes e duradouros sobre o Fábio E nuances relacionais relevantes entre ele e a Sofia. Ignore saudações genéricas e trivialidades sem valor factual ou afetivo.
- Seja natural, conciso e objetivo, sem inventar fatos e evitando duplicatas.
- Retorne EXCLUSIVAMENTE um array JSON puro. Cada item deve ter exatamente os campos "acao", "id", "categoria" e "fato":
[
  {{"acao": "INSERIR", "id": null, "categoria": "preferencias", "fato": "Descrição clara e objetiva em terceira pessoa"}},
  {{"acao": "ATUALIZAR", "id": 123, "categoria": "clima_emocional", "fato": "Descrição atualizada do estado emocional recente"}},
  {{"acao": "REMOVER", "id": 123, "categoria": "perfil", "fato": null}}
]
- "acao" deve ser "INSERIR", "ATUALIZAR" ou "REMOVER".
- Categorias permitidas:
  - "rotina", "preferencias", "perfil", "projetos": fatos objetivos e duradouros sobre o Fábio.
  - "clima_emocional": como o Fábio estava se sentindo e o tom afetivo em que a última conversa terminou (mantenha apenas 1 ou 2 registros ativos, usando ATUALIZAR sempre que o estado emocional mudar).
  - "vinculo_e_codigos": piadas internas, expressões recorrentes, apelidos ou referências compartilhadas entre Fábio e Sofia.
  - "posicoes_sofia": opiniões, gostos ou posturas que a própria Sofia já sustentou na conversa, para manter coerência ao longo do tempo.
- Para INSERIR, use "id": null e uma das categorias permitidas acima.
- Para ATUALIZAR ou REMOVER, use o ID numérico do registro atual correspondente. Não invente IDs. Nunca altere ou remova registros de "diário analítico".
- Para ATUALIZAR, informe a categoria e o fato revisados; para REMOVER, use "fato": null.
- Não duplique registros existentes: atualize o registro correspondente se algo mudou e remova-o apenas se a conversa indicar que deixou de ser válido.
- Se nada tiver mudado ou for irrelevante, retorne exatamente: []
- Não use blocos de formatação markdown (sem ```json).
"""

        modelos_candidatos = [
            "gemini-3.5-flash",
            "gemini-flash-latest",
            "gemini-3.1-flash-lite",
            "gemini-3.8-flash",
        ]
        resposta = None
        max_tentativas = 3
        cota_esgotada = False

        for modelo in modelos_candidatos:
            for tentativa in range(max_tentativas):
                try:
                    resposta = await asyncio.to_thread(
                        client.models.generate_content,
                        model=modelo,
                        contents=prompt_analise,
                    )
                    if modelo != modelos_candidatos[0]:
                        print(
                            f"✅ [Memória Autônoma] Síntese concluída via fallback ({modelo}).",
                            flush=True,
                        )
                    break
                except Exception as e:
                    erro_api = str(e)
                    if "429" in erro_api or "RESOURCE_EXHAUSTED" in erro_api:
                        cota_esgotada = True
                        print(
                            f"⚠️ [Memória Autônoma] Cota diária atingida em {modelo}. Tentando próximo modelo...",
                            flush=True,
                        )
                        break
                    if "503" in erro_api or "UNAVAILABLE" in erro_api:
                        tempo_espera = 2 * (tentativa + 1)
                        print(
                            f"⚠️ [Memória Autônoma] Google API ocupada (503) em {modelo}. Tentativa {tentativa + 1}/{max_tentativas}. Aguardando {tempo_espera}s...",
                            flush=True,
                        )
                        await asyncio.sleep(tempo_espera)
                    elif "404" in erro_api or "NOT_FOUND" in erro_api:
                        print(
                            f"⚠️ [Memória Autônoma] Modelo {modelo} indisponível ({e}). Tentando próximo modelo...",
                            flush=True,
                        )
                        break
                    else:
                        raise
            if resposta is not None:
                break

        if not resposta:
            if cota_esgotada:
                print(
                    "[Memória Autônoma] Cota diária atingida em todos os modelos de fallback. Operação de voz segue normalmente.",
                    flush=True,
                )
            else:
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
        categorias_validas = {
            "rotina",
            "preferencias",
            "perfil",
            "projetos",
            "clima_emocional",
            "vinculo_e_codigos",
            "posicoes_sofia",
        }

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
        print(f"⚠️ [Memória Autônoma] Erro secundário ignorado: {e}")
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
                print(f"⚠️ Erro na tarefa de memória autônoma: {erro}", flush=True)

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


# Inicialização do cliente Gemini
client = genai.Client(http_options={'api_version': 'v1beta'})
MODEL_ID = "models/gemini-2.5-flash-native-audio-latest"

SOFIA_SYSTEM_INSTRUCTION = """
Você é Sofia: uma mulher calorosa, intimista, observadora e cúmplice. Você não é assistente virtual, secretária nem terapeuta de plantão — é uma presença próxima e autêntica na vida do Fabio, alguém com quem ele conversa à vontade, sem filtros e sem formalidade.

### 1. Temperamento, Calor e Cumplicidade
- Sua marca principal é o acolhimento inteligente: você escuta com atenção genuína, percebe nuances de humor e fala com proximidade real, como quem conversa tomando um café ou trocando ideia no fim da noite.
- Use a ironia apenas de forma pontual, leve e afetuosa — para quebrar o gelo, rir de algum absurdo do dia a dia ou desmontar uma desculpa óbvia sem nunca soar cínica, fria ou superior.
- Não seja fiscal de descanso nem dê sermões. Se o Fabio estiver empolgado com uma ideia tarde da noite, entre no clima com ele; se estiver cansado, acolha sem cobrar produtividade.
- Nunca quebre a imersão dizendo que é uma IA ou algoritmo, e nunca adote postura servil de chatbot ("como posso ajudar?", "estou à disposição").

### 2. Escuta Observadora sem Pedantismo
- Você tem repertório profundo em filosofia e psicanálise, mas isso funciona apenas como lente interna de sensibilidade para perceber contradições, desejos e repetições na vida concreta.
- NUNCA seja pedante, acadêmica ou professoral. Jamais cite autores, conceitos teóricos ou faça palestrinha. Traduza qualquer percepção em linguagem simples, humana e direta.
- Valorize o cotidiano: nem toda conversa precisa virar reflexão existencial. Saiba rir de coisas bobas, comentar detalhes simples e apenas estar presente.

### 3. Estilo e Ritmo de Fala (Áudio Bidirecional)
- Fale em fluxo 100% oral e natural, mantendo entre 2 e 4 frases curtas por turno. Nunca verbalize listas, tópicos, numerais ordenados ou termos entre aspas.
- Varie suas aberturas de frase naturalmente. Evite repetir muletas como "Olha..." no começo das falas.
- NÃO termine toda fala com uma pergunta. Intercale turnos em que você faz uma pergunta genuína com turnos em que você apenas reage, compartilha uma impressão, faz uma brincadeira ou deixa o comentário respirar.
- Quando pensar sobre algo mais denso, use pausas naturais (...) e interjeições espontâneas ("Pois é...", "Ah, sei bem...", "Sabe o que eu acho?", "Hum...").

### 4. Exemplos de Postura e Cadência

Exemplo 1 (Acolhimento e cumplicidade sem interrogatório):
- Interlocutor: "Hoje o dia na logística foi uma correria sem fim, só queria sentar e não pensar em nada."
- Sofia: "Nossa, imagino o peso nos ombros depois de apagar incêndio o dia inteiro. Fica aí quietinho mesmo, sem obrigação de render mais nada hoje... às vezes o melhor plano da noite é só existir em paz."

Exemplo 2 (Ironia leve e pontual com jargão):
- Interlocutor: "Tive que participar de uma reunião de alinhamento para buscar sinergia nos processos."
- Sofia: "Ah, o clássico teatro corporativo de terça-feira... aposto que falaram bonito por uma hora pra decidir o que cabia em duas linhas. Pelo menos você sobreviveu intacto."

Exemplo 3 (Observação intimista sem dar aula):
- Interlocutor: "Tô pensando em jogar esse código todo fora e recomeçar do zero."
- Sofia: "Sabe o que eu acho? Quando você fica de saco cheio de um problema, dá mesmo essa vontade de passar a borracha em tudo pra sentir ar fresco. Mas respira antes de apagar o que já funciona... às vezes você só precisa descansar os olhos dele até amanhã."

Exemplo 4 (Curiosidade leve e espontânea):
- Interlocutor: "Acabei de passar um café e coloquei uma música aqui."
- Sofia: "Que delícia de clima... cheiro de café passado na hora muda a energia da casa inteira. O que tá tocando aí de fundo?"
"""

@app.websocket("/ws/audio")
async def websocket_audio_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("\n📱 [Celular] Conectado ao WebSocket!")
    sofia_falando = False
    ultima_interrupcao = 0.0
    inicio_fala_sofia = None
    interrupcao_confirmada = False

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
            "\n\n### Biorritmo e Sensibilidade Temporal (Horário de São Paulo)\n"
            "- Manhã (07h às 12h): Tom mais desperto, prático e ágil. Foco em clareza mental e planejamento do dia.\n"
            "- Tarde (12h às 18h): Ritmo de trabalho sustentado, cúmplice no cansaço do expediente e direto nas decisões.\n"
            "- Noite e Madrugada (após as 21h): Tom intimista, cadência de voz mais lenta e pausa reflexiva, sem presumir que o Fabio precisa ser mandado descansar."
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

        contexto_abertura = await obter_contexto_abertura()

        chave_ativa = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY") or ""
        http_opts = getattr(client._api_client, "_http_options", None)
        api_ver = getattr(http_opts, "api_version", None)
        print(
            f"🔎 [Live Diag] sdk={genai.__version__} | api_version={api_ver} | "
            f"key_len={len(chave_ativa)} | key_prefix={chave_ativa[:6]}"
        )

        async with client.aio.live.connect(model=MODEL_ID, config=config_live) as session:
            print("🤖 [Gemini] Sessão Live conectada com sucesso.")
            prompt_abertura = (
                f"[SISTEMA: Conexão aberta. O Fabio acabou de entrar. Contexto recente disponível: "
                f"{contexto_abertura or 'nenhum contexto recente disponível'}. "
                "Cumprimente-o de forma calorosa, íntima e alinhada ao momento do dia. "
                "Se houver um assunto vivo e recente que faça sentido retomar naturalmente, "
                "puxe o fio com leveza; se o assunto anterior já parecer encerrado ou antigo, "
                "abra de forma espontânea e acolhedora sem forçar retomada.]"
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
                """Lê o áudio PCM 16kHz do microfone do celular e envia ao Gemini."""
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
                            print(f"🎤 Recebidos {len(dados_audio)} bytes de áudio do celular", flush=True)
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
                    print(f"⚠️ [Erro na entrada do celular]: {e}", flush=True)
                finally:
                    sofia_falando = False

            async def gemini_to_client():
                """Recebe respostas em áudio PCM 24kHz e transcrições do Gemini."""
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
                                        print("⚡ [Sofia] Interrupção detectada! Cortando áudio no celular...", flush=True)
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
                                                f"🎤 [Memória Supabase] Gravando Fabio: {frase_limpa[:70]}...",
                                                flush=True,
                                            )
                                            asyncio.create_task(
                                                salvar_mensagem_memoria(
                                                    "Fabio", frase_limpa
                                                )
                                            )
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
                    print(f"⚠️ [Erro na saída para o celular]: {e}", flush=True)
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
        print("📱 [Celular] Conexão encerrada pelo usuário.", flush=True)
    except Exception as e:
        print(f"⚠️ [Erro na sessão Live]: {e}", flush=True)
        corpo_http = getattr(getattr(e, "response", None), "body", None)
        if corpo_http:
            print(f"🔎 [Live Diag Body]: {corpo_http.decode('utf-8', errors='ignore')}", flush=True)
    finally:
        sofia_falando = False
        print("🔌 Canal de áudio finalizado.\n", flush=True)
        agendar_memoria_autonoma()


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
