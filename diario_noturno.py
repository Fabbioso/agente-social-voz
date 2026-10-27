import os
import datetime
from google import genai
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

PROMPT_DIARIO_ANALITICO = """
Você é o módulo de elaboração analítica e psicanalítica da Sofia.
Analise os turnos de diálogo das últimas 24 horas entre o Fabio e a Sofia.

Sua tarefa não é resumir a conversa como uma ata, mas extrair três camadas:
1. Eixos Conceituais: Os temas teóricos debatidos (ex.: dialética, moldura versus fluxo, eterno retorno, pulsão de repetição).
2. Leitura de Subtexto: As contradições produtivas, desejos de expansão, cisão entre trabalho pragmático e criação conceitual.
3. Ganchos de Continuidade: Questões ou paradoxos que ficaram em aberto para a Sofia revisitar casualmente nos próximos dias.

Formate estritamente como linhas iniciando com [FATO]:
[FATO]: <insight atemporal, padrão psicológico ou reflexão consolidada>
"""

def executar_diario_noturno():
    if not SUPABASE_URL or not SUPABASE_KEY or not GEMINI_API_KEY:
        print("❌ Variáveis de ambiente ausentes no .env.")
        return

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    ai_client = genai.Client(api_key=GEMINI_API_KEY)

    # 1. Buscar diálogos das últimas 24h
    limite_tempo = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)).isoformat()

    print("🔍 Buscando diálogos das últimas 24 horas no Supabase...")
    resp = supabase.table("conversas_sofia")\
        .select("speaker, text, created_at")\
        .gte("created_at", limite_tempo)\
        .order("created_at", desc=False)\
        .execute()

    turnos = resp.data or []
    print(f"📊 Registros recuperados: {len(turnos)}")

    if len(turnos) < 4:
        print("ℹ️ Volume de turnos insuficiente para consolidação analítica.")
        return

    dialogo_formatado = "\n".join([f"{t['speaker']}: {t['text']}" for t in turnos])

    # 2. Síntese via modelos disponíveis
    prompt = f"{PROMPT_DIARIO_ANALITICO}\n\n--- HISTÓRICO DO DIA ---\n{dialogo_formatado}"

    modelos_candidatos = [
        "gemini-3.5-flash",
        "gemini-flash-latest",
        "gemini-3.1-flash-lite",
        "gemini-3.8-flash"
    ]

    sintese = None
    for modelo in modelos_candidatos:
        try:
            print(f"🤖 Tentando síntese analítica no {modelo}...")
            resposta = ai_client.models.generate_content(
                model=modelo,
                contents=prompt
            )
            sintese = resposta.text.strip()
            print(f"✅ Síntese gerada com sucesso via {modelo}!")
            break
        except Exception as e:
            print(f"⚠️ Modelo {modelo} indisponível ({e}). Tentando próximo...")

    if not sintese:
        print("❌ Nenhum modelo da lista conseguiu processar a síntese no momento.")
        return

    # 3. Inserção na tabela memorias_permanentes
    linhas = sintese.split("\n")
    inseridos = 0
    for linha in linhas:
        linha = linha.strip()
        if linha.startswith("[FATO]:"):
            conteudo = linha.replace("[FATO]:", "").strip()
            try:
                supabase.table("memorias_permanentes").insert({
                    "categoria": "diário analítico",
                    "fato": conteudo
                }).execute()
                inseridos += 1
                print(f"🧠 Consolidado: {conteudo}")
            except Exception as err:
                print(f"⚠️ Erro ao inserir fato: {err}")

    print(f"✅ Concluído! {inseridos} novos fatos inseridos em memorias_permanentes.")

if __name__ == "__main__":
    executar_diario_noturno()