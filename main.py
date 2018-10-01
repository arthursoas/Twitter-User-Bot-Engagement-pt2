import conexaoTwitter
from twitter import error
import time
from datetime import datetime, timedelta
from time import strptime
import os
from pathlib import Path


class Coleta(object):
    def __init__(self):
        # ABERTURA DE CONEXÃO COM O TWITTER
        self.api = conexaoTwitter.Open()

        # CRIAÇÃO DE DICIONÁRIOS
        self.dicRetweets = {}
        self.dicSeguidores = {}

    def teste(self):
        pass

    def realizar_coleta(self):
        if self.api is not None:

            lista_de_bots = self.obter_bots()

            for bot in lista_de_bots:
                # DADOS DO BOT
                dados_bot = self.obter_dados_bot(bot)

                if dados_bot is None:
                    continue

                nome_bot = dados_bot[0]

                print("Coleta iniciada para o bot " + str(nome_bot))

                self.obter_seguidores(bot)

                for seguidor in self.dicSeguidores.keys():
                    self.dicRetweets[seguidor] = []

                posts = self.obter_posts_coletados(bot)
                posts = list(map(int, posts))  # CONVERTE ITENS DA LISTA PARA INTEIROS
                posts.sort()

                print(str(len(posts)) + " posts para analisar")

                # COLETA DE RETWEETS
                self.obter_retweets(posts)

                self.salvar_dicionario(bot, "retweets")
                print("Dados salvos")

                self.bot_coletado(bot)
                self.limpar_dicionarios()
                self.atualizar_limite('0')
                print("Coleta finalizada para o bot " + str(nome_bot) + ' | ' + str(bot))

        else:
            print("Erro ao acessar a API.")


    def obter_seguidores(self, bot):
        seguidores_vinculacao = open("ArquivosSaida/" + str(bot) + "/seguidoresVinculacao.txt", "r")
        lines = seguidores_vinculacao.readlines()
        seguidores_vinculacao.close()

        lines.pop(0)
        for line in lines:
            line_splitted = line.split(",")
            self.dicSeguidores[int(line_splitted[0])] = {
                'vinculacao': datetime.strptime(line_splitted[1].rstrip(), '%Y-%m-%d %H:%M:%S.%f'),
                'desvinculacao' : None
            }

        seguidores_desvinculacao = open("ArquivosSaida/" + str(bot) + "/seguidoresDesvinculacao.txt", "r")
        lines = seguidores_desvinculacao.readlines()
        seguidores_desvinculacao.close()

        lines.pop(0)
        for line in lines:
            line_splitted = line.split(",")
            self.dicSeguidores[int(line_splitted[0])]['desvinculacao'] = datetime.strptime(line_splitted[1].rstrip(),
                                                                                      '%Y-%m-%d %H:%M:%S.%f')



    # SALVA DICIONÁRIO COM OS DADOS COLETADOS
    def salvar_dicionario(self, bot, tipo):
        arquivo = open("ArquivosSaida/" + str(bot) + "/" + tipo + ".txt", "w")

        if tipo == "retweets":
            for retweet in self.dicRetweets:
                for i in range(0, len(self.dicRetweets[retweet])):
                    arquivo.write(str(retweet) + "," + str(self.dicRetweets[retweet][i]['id']) +
                                  "," + str(self.dicRetweets[retweet][i]['data']) + "\n")

        arquivo.close()


    def limpar_dicionarios(self):
        self.dicRetweets.clear()


    def obter_dados_bot(self, bot):
        try:
            objeto_bot = self.api.GetUser(user_id=bot)
            return [objeto_bot.screen_name, objeto_bot.created_at]
        except error.TwitterError as e:
            print("Erro durante coleta de dados do usuário: " + str(e.message))


    @staticmethod
    def obter_limite():
        try:
            file = open("ArquivosSaida/limite.txt", 'r')
            line = file.readline()
            file.close()

            if line != '':
                return int(line)
            else:
                return None

        except IOError as e:
            print("Erro ao ler os limites: " + str(e))


    @staticmethod
    def atualizar_limite(limite):
        arquivo = open("ArquivosSaida/limite.txt", "w")
        arquivo.write(str(limite))
        arquivo.close()


    @staticmethod
    def obter_posts_coletados(bot):
        try:
            file = open("ArquivosSaida/" + str(bot) + "/posts.txt", 'r')
            lines = file.readlines()
            file.close()

            id_posts = []
            for line in lines:
                line_splitted = line.split(",")
                id_posts.append(line_splitted[0])

            return id_posts
        except IOError as e:
            print("Erro ao ler os posts antigos: " + str(e))


    def obter_retweets(self, posts):
        try:
            for post in posts:
                horario_coleta = datetime.now()
                dados_post = self.api.GetStatus(status_id=post)

                if dados_post.retweet_count == 0:
                    horario_corrente = datetime.now()
                    tempo_processamento = (horario_corrente - horario_coleta).total_seconds()
                    {} if tempo_processamento > 1 else time.sleep(1 - tempo_processamento)
                    continue

                retweets_post = self.api.GetRetweets(statusid=post, count=100)
                for retweet in retweets_post:
                    if retweet.user.id in self.dicSeguidores.keys():
                        if self.converter_formato_data(retweet.created_at) >= self.dicSeguidores[retweet.user.id][
                            'vinculacao']:
                            if self.dicSeguidores[retweet.user.id]['desvinculacao'] is None:
                                self.dicRetweets[retweet.user.id].append(
                                    {'data': self.converter_formato_data(retweet.created_at),
                                     'id': retweet.retweeted_status.id})
                            elif self.converter_formato_data(retweet.created_at) <= self.dicSeguidores[retweet.user.id][
                                'desvinculacao']:
                                self.dicRetweets[retweet.user.id].append(
                                    {'data': self.converter_formato_data(retweet.created_at),
                                     'id': retweet.retweeted_status.id})

                horario_corrente = datetime.now()
                tempo_processamento = (horario_corrente - horario_coleta).total_seconds()
                {} if tempo_processamento > 1 else time.sleep(1 - tempo_processamento)
        except error.TwitterError as e:
            print("Erro durante coleta de retweets: " + str(e.message))


    @staticmethod
    def converter_formato_data(data_twitter):
        created = data_twitter.split(" ")
        date_time = created[5] + "-" + str(strptime(created[1], '%b').tm_mon) + "-" + created[2] + " " + created[3]
        return datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')


    @staticmethod
    def obter_bots():
        lines = []
        lines2 = []

        try:
            file = open("ArquivosEntrada/bots.txt", 'r')
            lines = file.readlines()
            print("Bots obtidos com sucesso")
            file.close()

            file2 = open("ArquivosSaida/botsColetados.txt", 'r')
            lines2 = file2.readlines()
            file2.close()
        except IOError as e:
            print("Erro ao obter os bots: " + str(e))

        bots_pendentes = []
        for t in lines:
            bot_coletado = False
            for a in lines2:
                bot_a = a.rstrip()
                bot_t = t.rstrip()
                if str(bot_a) == str(bot_t):
                    bot_coletado = True

            if bot_coletado is False:
                bots_pendentes.append(str(t).rstrip())

        return bots_pendentes


    @staticmethod
    def bot_coletado(bot):
        try:
            arquivo = open("ArquivosSaida/botsColetados.txt", "a")
            arquivo.write(str(bot) + "\n")
            arquivo.close()
        except IOError as e:
            print("Erro ao salvar bot como coletado: " + str(e))


    @staticmethod
    def busca_binaria(lista, valor):
        first = 0
        last = len(lista) - 1

        while first <= last:
            mid = (first + last) // 2
            if lista[mid] == valor:
                return True
            else:
                if valor < lista[mid]:
                    last = mid - 1
                else:
                    first = mid + 1
        return False

c = Coleta()
c.realizar_coleta()
