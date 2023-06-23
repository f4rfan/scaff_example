import sys


class Animal:
    numero_de_patas = 4

    def volar(self):
        print("Puedo volar Woody!")


class Gato(Animal):
    def __init__(self, nombre):
        self.nombre = nombre

    def hablar(self):
        print("miau")

    def show_number_of_feet(self):
        return self.numero_de_patas

    def vuela(self):
        self.volar()


def habla(x: Animal):
    print(x.hablar())


class Car:
    def __init__(self, key="000", max_kmh=180):
        self.__key = key
        self.max_kmh = max_kmh

    def get_key(self):
        return self.__key

    def show_information(self):
        print("llave: " + self.__key + "; vel. max: " + str(self.max_kmh))

    @staticmethod
    def saluda():
        print("Hola")


def main():
    felix = Gato("Felix")
    felix.vuela()

    car1 = Car(max_kmh=250)

    print(car1.get_key())
    print(car1.max_kmh)
    car1.show_information()
    car1.saluda()


if __name__ == "__main__":
    sys.exit((main()))
