from dotenv import load_dotenv
from google import genai


def main():
    load_dotenv()
    client = genai.Client()

    modelos_live = []

    print("Modelos disponíveis:")
    for modelo in client.models.list():
        nome = modelo.name or "(sem nome)"
        acoes = modelo.supported_actions or []
        compativel_live = "bidiGenerateContent" in acoes
        marcador = " [compatível com Live API]" if compativel_live else ""
        print(f"- {nome}{marcador}")

        if compativel_live:
            modelos_live.append(nome)

    print("\nModelos compatíveis com a Live API:")
    if modelos_live:
        for nome in modelos_live:
            print(f"- {nome}")
    else:
        print("Nenhum modelo compatível foi encontrado.")


if __name__ == "__main__":
    main()