from flask import Flask
from flask import request
from pymongo import MongoClient
import pandas as pd
import json
from flask_cors import CORS
import threading
import random
# Conexión al Server de MongoDB Pasandole el host y el puerto
mongoClient = MongoClient('10.10.150.11',27017,username='root',password='pass')
# Conexión a la base de datos
db = mongoClient.preciosMayoristas
# Obtenemos una coleccion
collection = db.precios
meses = collection.distinct("mes")
años = collection.distinct("anio")
class Server(object):
    
    
    app = Flask(__name__)
    CORS(app)
    
    @app.route('/api/grupos/', methods=['GET'])
    def getGrupos():
        
        grupos = list(collection.distinct("grupo"))
        return json.dumps(grupos)
        #return "Si es esta"
    
    @app.route('/api/productos/', methods=['POST'])
    def getProductos():
        
        queryRequest = request.get_json(silent=True)
        productos = list(collection.distinct("producto" , queryRequest))
        return json.dumps(productos)
    
    @app.route('/api/ciudades/', methods=['POST'])
    def getCiudades():
        
        queryRequest = request.get_json(silent=True)
        ciudades = list(collection.distinct("ciudad" , queryRequest))
        return json.dumps(ciudades)
    
    @app.route('/api/fuentes/', methods=['POST'])
    def getFuentes():
        
        queryRequest = request.get_json(silent=True)
        fuentes = list(collection.distinct("fuente" , queryRequest))
        return json.dumps(fuentes)
    
    @app.route('/api/prediccion/', methods=['POST'])
    def getPrediccion():
        queryRequest = request.json
        
        #Genera el promedio para cada año y cada mes del filtro de busqueda
        def getPromedio():   
            l=[]
            #Realiza el filtro por mes y por año y devuelve un objeto con esta infromacion
            def buscarProducto(mes,año):
                queryAdd = []
                queryAdd = [{"mes":mes},{"anio":año}]
                query=[]
                query.extend(queryRequest)
                query.extend(queryAdd)
                df = pd.DataFrame(list(collection.find({"$and": query},{"precio":1, "mes":1, "anio":1, "_id":0})))
                prom = {
                        "anio":año,
                        "mes":mes,
                        "precio":df['precio'].mean()
                        }
                l.append(prom)
            for mes in meses:
                hilo1 = threading.Thread(name='hilo%s' %mes+'2013',target=buscarProducto, args=(mes,2013))
                hilo2 = threading.Thread(name='hilo%s' %mes+'2014',target=buscarProducto, args=(mes,2014))
                hilo3 = threading.Thread(name='hilo%s' %mes+'2015',target=buscarProducto, args=(mes,2015))
                hilo4 = threading.Thread(name='hilo%s' %mes+'2016',target=buscarProducto, args=(mes,2016))
                hilo5 = threading.Thread(name='hilo%s' %mes+'2017',target=buscarProducto, args=(mes,2017))
                hilo6 = threading.Thread(name='hilo%s' %mes+'2018',target=buscarProducto, args=(mes,2018))
                hilo1.start()
                hilo2.start()
                hilo3.start()
                hilo4.start()
                hilo5.start()
                hilo6.start()
                hilo1.join()
                hilo2.join()
                hilo3.join()
                hilo4.join()
                hilo5.join()
                hilo6.join()
             
            dx = pd.DataFrame(list(l))
            
            respuesta = dx
            return respuesta
        
        #Invoca el metodo del promedio y con su respuesta realiza la prediccion de los precios
        def getPrediccion(): 
            sumMes=[]
            lista = getPromedio()
            
            sumMes = lista.groupby(['mes'])['precio'].sum()
            sumTotal = lista['precio'].sum()
            feMes=[]
            for i in range(1, 13):
                f = (sumMes[i]/len(lista[lista['mes']==i]))/sumTotal
                feMes.append(f)
            
            for i in range(0,len(lista)):
                lista.loc[i:i, 'factorEstacional'] = feMes[int(lista.loc[i:i, 'mes']-1)]
                lista.loc[i:i, 'precioDesestaciona'] = (lista.loc[i:i, 'precio']*feMes[int(lista.loc[i:i, 'mes']-1)])
                lista.loc[i:i, 'precioDesElevado'] = (lista.loc[i:i, 'precioDesestaciona'])**2
            
            tabla=[]
            for i in range(1, 13):
                mes = lista[lista['mes']==i].sort_values(['anio', 'mes'], ascending=[True, True]).reset_index()
                x=0
                y=0
                x2=0
                y2=0
                xxy=0
                for j in range(0,(len(mes))):
                    x += (j+1)
                    y += float(mes.loc[j:j,'precioDesestaciona'])
                    x2 += (j+1)**2
                    y2 += float(mes.loc[j:j,'precioDesestaciona'])**2
                    xxy += (j+1)*float(mes.loc[j:j,'precioDesestaciona'])
                mes={
                        "mes":i,
                        "x":x,
                        "y":y,
                        "x2":x2,
                        "y2":y2,
                        "xxy":xxy,
                        "anioMax": int(mes.loc[len(mes)-1:len(mes)-1,'anio'])
                    }
                tabla.append(mes)
                
            tablaP = pd.DataFrame(list(tabla))
            
            for i in range(0,12):
                b = (((len(lista[lista['mes']==i+1]))*float(tablaP.loc[i:i,'xxy']))-(int(tablaP.loc[i:i,'x'])*float(tablaP.loc[i:i,'y'])))/(((len(lista[lista['mes']==i+1]))*int(tablaP.loc[i:i,'x2']))-(int(tablaP.loc[i:i,'x'])**2))
                a = (float(tablaP.loc[i:i,'y']) - (b*int(tablaP.loc[i:i,'x'])))/len(lista[lista['mes']==i+1])
                y = a + b* (len(lista[lista['mes']==i])+1)
                tablaP.loc[i:i,'desE'] = y
            
            listaFinal = lista.drop(['factorEstacional','precioDesestaciona','precioDesElevado'], axis=1)
            
            for i in range(0,len(tablaP)):
                tablaP.loc[i:i,'prrecioproyec'] = tablaP.loc[i:i,'desE']/feMes[i]
            
            
            for i in range(0,len(tablaP)):
                anoSig=int(tablaP.loc[i:i,'anioMax']+1)
                m = int(tablaP.loc[i:i,'mes'])
                p = float(tablaP.loc[i:i,'prrecioproyec'])
                listaFinal.loc[len(listaFinal)] = [anoSig,m,p]
                
    #        anios=[]
    #        for i,j in res.iterrows():
    #            if(int(j['anio']) not in anios):
    #                anios.append(int(j['anio']))
            anios = pd.unique(listaFinal['anio']).tolist()
            anios.sort()
            arreglo = []
            for anio in anios:
                valores = listaFinal[listaFinal['anio'] == anio]
                ran = random.randrange(10**80)
                myhex = "%064x" % ran
                myhex = myhex[:6]
                obj = {
                        "label":str(int(anio)),
                        "data":valores['precio'].tolist(),
                        "backgroundColor": '#'+myhex,
                        "borderColor": '#'+myhex,
                        "fill": False
                        }
                arreglo.append(obj)
    
            return arreglo
        
        res = getPrediccion()
        return json.dumps(res)
    
    if __name__ == '__main__':
       #app.run(debug=True)
    	app.run(host='10.10.150.11')
