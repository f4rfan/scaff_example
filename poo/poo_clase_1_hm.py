import sys


class Rutinas:
    @staticmethod
    def comer():
        print("comiendo")

    @staticmethod
    def entrenar():
        print("entrenando")

    @staticmethod
    def dormir():
        print("durmiendo")


class Corredor(Rutinas):
    def __init__(self, km_correr):
        self.km_correr = km_correr

    def accion(self):
        print("Corriendo")


class Nadador(Rutinas):
    def __init__(self, km_nadar):
        self.km_nadar = km_nadar

    def accion(self):
        print("Nadando")


class Atleta(Nadador, Corredor):
    def __init__(self, name, km_c=10, km_n=5):
        Corredor.__init__(self, km_c)
        Nadador.__init__(self, km_n)
        self.nombre = name

    def imprime_datos(self):
        print("Nombre: " + self.nombre)
        print("Km corridos: " + str(self.km_correr))
        print("Km nadados: " + str(self.km_nadar))


def main():
    atleta_1 = Atleta("Alfredo")
    atleta_1.imprime_datos()
    atleta_1.accion()
    atleta_1.comer()
    atleta_1.dormir()
    atleta_1.entrenar()


if __name__ == "__main__":
    sys.exit((main()))
