import sys


class Vehiculo:
    def __init__(self, en_movimiento=False, pasajeros=1):
        self.__en_movimiento = en_movimiento
        self.pasajeros = pasajeros

    def __moverse(self):
        if self.en_movimiento is False:
            self.en_movimiento = True
            print("Avanzando")
        else:
            print("Ya estamos avanzando")

    def __detenerse(self):
        if self.en_movimiento is True:
            self.en_movimiento = False
            print("Deteniendo")
        else:
            print("Ya estamos detenidos")


class Acuatico(Vehiculo):
    def __init__(self, tipo_viaje="Sobre el agua", tipo_impulso="propelas", accion="avanzar"):
        Vehiculo.__init__(self)
        self.tipo_viaje = tipo_viaje
        self.tipo_impulso = tipo_impulso
        self.accion = accion

    def __accion(self):
        if self.accion == "avanzar":
            self.__moverse()
        elif self.accion == "detenerse":
            self.__detenerse()
        else:
            print("no se puede realizar esa accion")

    def __abordar_vehiculo(self):
        self.pasajeros += 1


class Terrestre(Vehiculo):
    def __init__(self, tipo_viaje="Sobre la tierra", tipo_impulso="llantas", accion="avanzar"):
        Vehiculo.__init__(self)
        self.tipo_viaje = tipo_viaje
        self.tipo_impulso = tipo_impulso
        self.accion = accion

    def accion(self):
        if self.accion == "avanzar":
            self.__moverse()
        elif self.accion == "detenerse":
            self.__detenerse()
        else:
            print("no se puede realizar esa accion")

    def abordar_vehiculo(self):
        self.pasajeros += 1


class Barco(Acuatico):
    def __init__(self, impulso="Velas", vel_max=70, max_pasajeros=50):
        Acuatico.__init__(self, tipo_impulso=impulso)
        self.vel_max = vel_max
        self.__max_pasajeros = max_pasajeros

    def abordar_todos(self):
        for i in range(self.__max_pasajeros):
            self.__abordar_vehiculo()

    @staticmethod
    def viajar():
        print("Al abordaje!!")


class Submarino(Acuatico):
    def __init__(self, vel_max=40, max_pasajeros=30):
        Acuatico.__init__(self, tipo_viaje="Bajo el agua")
        self.vel_max = vel_max
        self.max_pasajeros = max_pasajeros

    @staticmethod
    def viajar():
        print("A nadar!")

    def abordar_todos(self):
        for i in range(self.__max_pasajeros):
            self.__abordar_vehiculo()


class Combi(Terrestre):
    def __init__(self, vel_max=120, max_pasajeros=15):
        Terrestre.__init__(self)
        self.vel_max = vel_max
        self.max_pasajeros = max_pasajeros

    def abordar_todos(self):
        for i in range(self.__max_pasajeros):
            self.__abordar_vehiculo()

    @staticmethod
    def viajar():
        print("A la base!!")


class Bicicleta(Terrestre):
    def __init__(self, vel_max=35, max_pasajeros=2):
        Terrestre.__init__(self)
        self.vel_max = vel_max
        self.max_pasajeros = max_pasajeros

    def abordar_todos(self):
        for i in range(self.__max_pasajeros):
            self.__abordar_vehiculo()

    @staticmethod
    def viajar():
        print("A rodar!!")


def main():
    vehiculo = Vehiculo()
    print(vehiculo.en_movimiento)
    vehiculo.moverse()
    print(vehiculo.en_movimiento)
    vehiculo.moverse()


if __name__ == "__main__":
    sys.exit((main()))
