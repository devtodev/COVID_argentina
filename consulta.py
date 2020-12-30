import mysql.connector
import json
import numpy as np
import time

file_zonas = "data_zonas.json"
file_totales = "data_totales.json"
file_edades = "data_provincias_edades.json"
TAG_COVIDPOSITIVO = "totales"
TAG_TESTS = "test"

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="C4l1f0rn14",
    database="mapa"
)


def consulta(query):
    mycursor = mydb.cursor()
    mycursor.execute(query)
    return mycursor.fetchall()


def consultaGeograficaFallecidos(geografia, fallecido, covid, fecha, tag):
    query = 'select residencia_provincia_id, residencia_departamento_id, count(*) as cantidad '
    query = query + 'from casos where upper(fallecido) = "'+fallecido+'"  and upper(clasificacion_resumen) = "'+covid+'" '
    if fecha == '>':
        query = query + 'and fecha_inicio_sintomas >= CURDATE() - INTERVAL 30 DAY '
    if fecha == '<':
        query= query + 'and fecha_inicio_sintomas < CURDATE() - INTERVAL 30 DAY '
    query = query + ' group by ' + geografia + ', residencia_provincia_id;'
    respuesta = {}
    for row in consulta(query):
        if geografia == "residencia_provincia_id":
            in1 = row[0]
        else:
            in1 = row[0] + row[1]
        respuesta[in1] = {TAG_COVIDPOSITIVO: { tag: row[2] }}
    return respuesta

def consultaGeograficaCuidados(geografia, cuidado, covid, tag):
    query = 'select residencia_provincia_id, residencia_departamento_id, count(*) as cantidad '
    query = query + 'from casos where upper(fallecido) = "NO" and upper(cuidado_intensivo) = "'+cuidado+'"  and upper(clasificacion_resumen) = "CONFIRMADO" and upper(clasificacion_resumen) = "'+covid+'" '
    query = query + 'group by ' + geografia + ', residencia_provincia_id;'
    respuesta = {}
    for row in consulta(query):
        if geografia == "residencia_provincia_id":
            in1 = row[0]
        else:
            in1 = row[0] + row[1]
        respuesta[in1] = {TAG_COVIDPOSITIVO: { tag: row[2] }}
    return respuesta

def consultaGeograficaCOVIDPositivo(geografia):
    respuesta = {}
    fallecidos = consultaGeograficaFallecidos(geografia, "SI", "CONFIRMADO", "" ,"Fallecidos")
    curados = consultaGeograficaFallecidos(geografia, "NO", "CONFIRMADO", "<", "curados")
    covid = consultaGeograficaFallecidos(geografia, "NO", "CONFIRMADO", ">", "COVID+")
    cuidados = consultaGeograficaCuidados(geografia, "SI", "CONFIRMADO", "Cuidados")
    respuesta = mergeDics(fallecidos, curados)
    respuesta = mergeDics(respuesta, cuidados)
    respuesta = mergeDics(respuesta, covid)
    return respuesta

def consultaGeografica(query, geografia, tag, subtag):
    respuesta = {}
    for row in consulta(query):
        if geografia == "residencia_provincia_id":
            in1 = row[0]
        else:
            in1 = row[0] + row[1]
        respuesta[in1] = {tag : {subtag: row[2]} }
    return respuesta

def consultaGeograficaAgrupadaClasificada(geografia, agrupacion, clasificacion):
    query = 'select residencia_provincia_id, residencia_departamento_id, count(*) '
    query = query + 'from casos where ' + agrupacion + ' = "' + clasificacion + '" '
    query = query + 'group by ' + geografia + ',residencia_provincia_id;'
    respuesta = consultaGeografica(query, geografia, TAG_TESTS, clasificacion)
    return respuesta

def consultaGeograficaAsistenciaRespiratoria(geografia):
    query = "select residencia_provincia_id, residencia_departamento_id, count(*)  from casos where asistencia_respiratoria_mecanica = 'SI' and fallecido = 'NO' and clasificacion_resumen = 'Confirmado' group by " + geografia + ",residencia_provincia_id;"
    respuesta = consultaGeografica(query, geografia, "respitador" ,"+")
    query = "select residencia_provincia_id, residencia_departamento_id, count(*)  from casos where asistencia_respiratoria_mecanica = 'SI' and fallecido = 'NO' and clasificacion_resumen != 'Confirmado' group by " + geografia + ",residencia_provincia_id;"
    sinCovidSinRespirador = consultaGeografica(query, geografia, "respitador", "-")
    respuesta = mergeDics(respuesta, sinCovidSinRespirador)
    return respuesta

def consultaGeograficaOrigenFinanciamiento(geografia):
    respuesta = {}
    query = "select residencia_provincia_id, residencia_departamento_id, count(*) from casos where origen_financiamiento = 'Privado' group by " + geografia + ",residencia_provincia_id;"
    privado = consultaGeografica(query, geografia, "Financiamiento" ,"Privado")
    query = "select residencia_provincia_id, residencia_departamento_id, count(*) from casos where origen_financiamiento != 'Privado' group by " + geografia + ",residencia_provincia_id;"
    respuesta = mergeDics(privado, consultaGeografica(query, geografia, "Financiamiento" ,"Público"))
    return respuesta

def consultaPoblacion():
    respuesta = {}
    query = "  select provincia_id, departamento_id, sexo, a2020 from poblacion group by provincia_id, departamento_id, sexo;"
    for row in consulta(query):
        in1 = row[0] + row[1]
        if not in1 in respuesta:
            respuesta[in1] = {}
        respuesta[in1][row[2]] = row[3]
    return respuesta

def consultaTotalAgrupadaClasificada(agrupacion, clasificacion):
    query = 'select count(*) as "' + clasificacion + '" '
    query = query + 'from casos where ' + agrupacion + ' = "' + clasificacion + '"; '
    row = consulta(query)
    respuesta = {clasificacion: row[0][0]}
    return respuesta

def mergeDics(a, b, path=None):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                mergeDics(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
#                if (key != "nombre"):
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a

def calculoQuartiles(query):
    edadesDepartamento = [int(i[0], 10) for i in consulta(query)]
    respuesta = {}
    if len(edadesDepartamento) == 0:
        edadesDepartamento = [0]
        respuesta = {"min": 0, "q1": 0, "q2": 0, "max": 0}
    else:
        if len(edadesDepartamento) == 2:
            respuesta = {"min": int(np.min(edadesDepartamento)), "q1": int(np.min(edadesDepartamento)), "q2": int(np.max(edadesDepartamento)), "max": int(np.max(edadesDepartamento))}
        else:
            respuesta = {"min": int(np.min(edadesDepartamento)), "q1": int(np.percentile(edadesDepartamento, 25)), "q2": int(np.percentile(edadesDepartamento, 75)), "max": int(np.max(edadesDepartamento))}
    return respuesta

def calculoEdadesEspecifico(in1Unificado):
    respuesta = {}
    in1Provincia = in1Unificado[0:2]
    in1Departamento = 0
    if (len(in1Unificado) > 2):
        in1Departamento = in1Unificado[2:5]
    # provincia edades positivo
    queryEdadPorDepartamento = 'select edad from casos where edad_años_meses = "años" and edad != "" and clasificacion_resumen = "Confirmado" and residencia_provincia_id = "'+in1Provincia+'"'
    queryEdadPorDepartamento = queryEdadPorDepartamento + 'and fallecido = "NO" and fecha_inicio_sintomas >= CURDATE() - INTERVAL 30 DAY '
    if len(in1Unificado) > 2:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ' and residencia_departamento_id = "' + in1Departamento + '";'
    else:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ';'

    respuesta["COVID+"] = calculoQuartiles(queryEdadPorDepartamento)
    # provincia edades curados
    queryEdadPorDepartamento = 'select edad from casos where edad_años_meses = "años" and edad != "" and clasificacion_resumen = "Confirmado" and residencia_provincia_id = "'+in1Provincia+'"'
    queryEdadPorDepartamento = queryEdadPorDepartamento + 'and fallecido = "NO" and fecha_inicio_sintomas < CURDATE() - INTERVAL 30 DAY '
    if len(in1Unificado) > 2:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ' and residencia_departamento_id = "' + in1Departamento + '";'
    else:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ';'
    respuesta["curados"] = calculoQuartiles(queryEdadPorDepartamento)
    # falta fallecidos
    queryEdadPorDepartamento = 'select edad from casos where edad_años_meses = "años" and edad != "" and clasificacion_resumen = "Confirmado" and residencia_provincia_id = "'+in1Provincia+'" and fallecido = "SI" '
    if len(in1Unificado) > 2:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ' and residencia_departamento_id = "' + in1Departamento + '";'
    else:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ';'

    respuesta["fallecidos"] =  calculoQuartiles(queryEdadPorDepartamento)

    # cuidados intensivos
    queryEdadPorDepartamento = 'select edad from casos where edad_años_meses = "años" and edad != "" and fallecido = "NO" and residencia_provincia_id = "'+in1Provincia+'" and cuidado_intensivo = "SI" and upper(clasificacion_resumen) = "CONFIRMADO" '
    if len(in1Unificado) > 2:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ' and residencia_departamento_id = "' + in1Departamento + '";'
    else:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ';'
    respuesta["cuidados"] =  calculoQuartiles(queryEdadPorDepartamento)

    # Respirador +
    queryEdadPorDepartamento = 'select edad from casos where edad_años_meses = "años" and edad != "" and fallecido = "NO" and residencia_provincia_id = "'+in1Provincia+'" and asistencia_respiratoria_mecanica = "SI" and upper(clasificacion_resumen) = "CONFIRMADO" '
    if len(in1Unificado) > 2:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ' and residencia_departamento_id = "' + in1Departamento + '";'
    else:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ';'
    respuesta["respirador+"] = calculoQuartiles(queryEdadPorDepartamento)

    # Respirador -
    queryEdadPorDepartamento = 'select edad from casos where edad_años_meses = "años" and edad != "" and fallecido = "NO" and residencia_provincia_id = "'+in1Provincia+'" and asistencia_respiratoria_mecanica = "SI" and upper(clasificacion_resumen) <> "CONFIRMADO" '
    if len(in1Unificado) > 2:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ' and residencia_departamento_id = "' + in1Departamento + '";'
    else:
        queryEdadPorDepartamento = queryEdadPorDepartamento + ';'
    respuesta["respirador-"] = calculoQuartiles(queryEdadPorDepartamento)

    return respuesta

def consultaAgrupadaClasificada(geografia, agrupacion, clasificaciones):
    respuesta = {}
    for clasificacion in clasificaciones:
        if geografia == "":
            newdata = consultaTotalAgrupadaClasificada(agrupacion, clasificacion)
        else:
            newdata = consultaGeograficaAgrupadaClasificada(geografia, agrupacion, clasificacion)
        respuesta = mergeDics(respuesta, newdata)
    return respuesta


def consultaTotales(agrupacion, nombre):
    query = 'select ' + agrupacion + ', count(*) as "cantidad" '
    query = query + ' from casos group by ' + agrupacion + ';'
    respuesta = {}
    for row in consulta(query):
        if nombre != "":
            respuesta[row[0]] = {nombre: row[1]}
        else:
            respuesta[row[0]] = row[1]
    return respuesta


def persistir(nombre, datos):
    f = open(nombre, "w")
    f.write(json.dumps(datos, ensure_ascii=False))
    f.close()
    print("Guardado en ", nombre)
    print('>>> ',datos)

def consultaGeograficaMaximos(geografia, clasificacionCasos):
    query = 'select '+geografia+', count(*) as "cantidad" from casos' \
	+ ' where clasificacion_resumen = "Confirmado" and '+geografia+' != "SIN ESPECIFICAR" ' \
    + ' and fecha_inicio_sintomas >= CURDATE() - INTERVAL 30 DAY ' \
	+ ' group by '+geografia+', residencia_provincia_id, clasificacion_resumen ' \
    + ' order by cantidad desc limit 1; '
    myquery = consulta(query)[0]
    resultado = {clasificacionCasos: myquery[1]}
    return resultado

def calculaMaximosFacellidosGeografico(geografia):
    query = 'select '+geografia+', count(*) cantidad from casos ' \
    + ' where fallecido = "SI" and '+geografia+' != "SIN ESPECIFICAR" ' \
    + ' group by '+geografia+', residencia_provincia_id order by cantidad desc; '
    myquery = consulta(query)[0]
    resultado = {"Fallecidos": myquery[1]}
    return resultado

def consultaMaximos():
    respuesta = {}
    respuesta["departamento"] = consultaGeograficaMaximos("residencia_departamento_nombre", "COVID+")
    respuesta["provincia"] = consultaGeograficaMaximos("residencia_provincia_nombre", "COVID+")
    temp = {}
    temp["departamento"] = calculaMaximosFacellidosGeografico("residencia_departamento_nombre")
    temp["provincia"] = calculaMaximosFacellidosGeografico("residencia_provincia_nombre")
    respuesta = mergeDics(respuesta, temp)
    return respuesta

def calculaPorcentajesEspecifico(datos, in1, maximos):
    resultado = {}
    if len(in1) == 2:
        maximosZona = maximos["provincia"]
    else:
        maximosZona = maximos["departamento"]
    for clasificacion in maximosZona:
        if "totales" in datos and clasificacion in datos["totales"]:    
            porcentaje = int(datos["totales"][clasificacion] * 1000 / (maximosZona[clasificacion]))
        else:
            porcentaje = 0
        resultado[clasificacion] = porcentaje

    return resultado

def calculaCurva(in1, campo):
    query = 'select DATE_FORMAT('+campo+', "%d/%m"), count(*) cantidad from casos '
    query = query + ' where '+campo+' <> "" and clasificacion_resumen = "Confirmado" '
    if campo == 'fecha_fallecimiento':
        query = query + ' and fallecido = "SI" '
    if campo == 'fecha_diagnostico':
        query = query + ' and clasificacion_resumen = "Confirmado" '
    if in1 != "":
        query = query + 'and residencia_provincia_id = "' + in1[0:2] + '" '
    if (len(in1) > 2):
        query = query + 'and residencia_departamento_id = "' + in1[2:5] + '" ';
    query = query + ' group by '+campo+' order by '+campo+'; '
    fecha = {}
    for row in consulta(query):
        fecha[row[0]] = row[1]
    return fecha

def calculaByIn1(datos, clasificacionCasos):
    maximos = consultaMaximos()
    poblacion = consultaPoblacion()
    for in1 in datos:
        datos[in1]["pormil"] = calculaPorcentajesEspecifico(datos[in1], in1, maximos)
        datos[in1]["edad"] = calculoEdadesEspecifico(in1)
        datos[in1]["curvaf"] = calculaCurva(in1, 'fecha_fallecimiento')
        datos[in1]["curvac"] = calculaCurva(in1, 'fecha_diagnostico')
        if in1 in poblacion:
            datos[in1]["poblacion"] = poblacion[in1]


    return datos

def consultaActualizacion():
    query = "select DATE_FORMAT(ultima_actualizacion, '%d/%m/%Y') from casos order by ultima_actualizacion desc limit 1;"
    return consulta(query)[0][0]

def consultaPositivosTotales():
    respuesta = {}
    query = "select count(*) fallecidos from casos where fallecido = 'SI' and clasificacion_resumen = 'Confirmado';"
    respuesta["Fallecidos"] = consulta(query)[0][0]
    query = "select count(*) fallecidos from casos where fallecido = 'NO' and fecha_inicio_sintomas >= CURDATE() - INTERVAL 30 DAY and clasificacion_resumen = 'Confirmado';"
    respuesta["COVID+"] = consulta(query)[0][0]
    query = "select count(*) fallecidos from casos where fallecido = 'NO' and fecha_inicio_sintomas < CURDATE() - INTERVAL 30 DAY and clasificacion_resumen = 'Confirmado';"
    respuesta["curados"] = consulta(query)[0][0]
    query = "select count(*) cuidados from casos where cuidado_intensivo = 'SI' and fallecido = 'NO'  and upper(clasificacion_resumen) = 'CONFIRMADO';"
    respuesta["Cuidados"] = consulta(query)[0][0]
    return respuesta

def consultaAsistenciaRespiratoria():
    respuesta = {}
    query = "select count(*) cantidad from casos where asistencia_respiratoria_mecanica = 'SI' and fallecido = 'NO' and clasificacion_resumen = 'Confirmado';"
    respuesta["+"] = consulta(query)[0][0]
    query = "select count(*) cantidad from casos where asistencia_respiratoria_mecanica = 'SI' and fallecido = 'NO' and clasificacion_resumen != 'Confirmado';"
    respuesta["-"] = consulta(query)[0][0]
    return respuesta

def calculoEdadesTotal():
    # positivos
    respuesta = { "edad": {} }
    queryEdadPorDepartamento = 'select edad from casos where edad_años_meses = "años" and edad != "" and clasificacion_resumen = "Confirmado"'
    queryEdadPorDepartamento = queryEdadPorDepartamento + 'and fallecido = "NO" and fecha_inicio_sintomas >= CURDATE() - INTERVAL 30 DAY; '
    edades = [int(i[0], 10) for i in consulta(queryEdadPorDepartamento)]
    if len(edades) == 0:
        edades = [0]
    respuesta["edad"]["COVID+"] = {"min": int(np.min(edades)), "q1": int(np.percentile(edades, 25)), "q2": int(np.percentile(edades, 75)), "max": int(np.max(edades))}
    # curados
    queryEdadPorDepartamento = 'select edad from casos where edad_años_meses = "años" and edad != "" and clasificacion_resumen = "Confirmado"'
    queryEdadPorDepartamento = queryEdadPorDepartamento + ' and fallecido = "NO" and fecha_inicio_sintomas < CURDATE() - INTERVAL 30 DAY; '
    edades = [int(i[0], 10) for i in consulta(queryEdadPorDepartamento)]
    if len(edades) == 0:
        edades = [0]
    respuesta["edad"]["curados"] = {"min": int(np.min(edades)), "q1": int(np.percentile(edades, 25)), "q2": int(np.percentile(edades, 75)), "max": int(np.max(edades))}
    # fallecidos
    queryEdadPorDepartamento = 'select edad from casos where edad_años_meses = "años" and edad != "" and clasificacion_resumen = "Confirmado" and fallecido = "SI";'
    edades = [int(i[0], 10) for i in consulta(queryEdadPorDepartamento)]
    if len(edades) == 0:
        edades = [0]
    respuesta["edad"]["fallecidos"] = {"min": int(np.min(edades)), "q1": int(np.percentile(edades, 25)), "q2": int(np.percentile(edades, 75)), "max": int(np.max(edades))}
    # cuidados intensivos
    queryEdadPorDepartamento = 'select edad from casos where edad_años_meses = "años" and edad != "" and fallecido = "NO" and cuidado_intensivo = "SI"  and upper(clasificacion_resumen) = "CONFIRMADO";'
    edades = [int(i[0], 10) for i in consulta(queryEdadPorDepartamento)]
    if len(edades) == 0:
        edades = [0]
    respuesta["edad"]["cuidados"] = {"min": int(np.min(edades)), "q1": int(np.percentile(edades, 25)), "q2": int(np.percentile(edades, 75)), "max": int(np.max(edades))}
    # respiradores covid +
    queryEdadCovid = 'select edad from casos where edad_años_meses = "años" and edad != "" and fallecido = "NO" and asistencia_respiratoria_mecanica = "SI"  and upper(clasificacion_resumen) = "CONFIRMADO";'
    edades = [int(i[0], 10) for i in consulta(queryEdadCovid)]
    if len(edades) == 0:
        edades = [0]
    respuesta["edad"]["respirador+"] = {"min": int(np.min(edades)), "q1": int(np.percentile(edades, 25)),
                                        "q2": int(np.percentile(edades, 75)), "max": int(np.max(edades))}
    # respiradores covid +
    queryEdadNoCovid = 'select edad from casos where edad_años_meses = "años" and edad != "" and fallecido = "NO" and asistencia_respiratoria_mecanica = "SI"  and upper(clasificacion_resumen) <> "CONFIRMADO";'
    edades = [int(i[0], 10) for i in consulta(queryEdadNoCovid)]
    if len(edades) == 0:
        edades = [0]    
    respuesta["edad"]["respirador-"] = {"min": int(np.min(edades)), "q1": int(np.percentile(edades, 25)),
                                        "q2": int(np.percentile(edades, 75)), "max": int(np.max(edades))}
    return respuesta

def calculaTotales(clasificacionCasos):
    totales = calculoEdadesTotal()
    totales["test"] = consultaAgrupadaClasificada("", "clasificacion_resumen", clasificacionCasos)
    totales["totales"] = consultaPositivosTotales()
    totales["respitador"] = consultaAsistenciaRespiratoria()
    totales["Financiamiento"] = consultaTotales("origen_financiamiento", "")
    totales["Fecha actualizacion"] = {"Datos": consultaActualizacion(), "Sistema": "2020-07-28"}
    totales["curvaf"] = calculaCurva("", 'fecha_fallecimiento')
    totales["curvac"] = calculaCurva("", 'fecha_diagnostico')
    # totales["Por sexo"] = consultaTotales("sexo", "")
    persistir(file_totales, totales)
    return

def calculaZona(geografia, clasificacionCasos):
    respuesta = {}
    clasificacion = consultaAgrupadaClasificada(geografia, 'clasificacion_resumen', clasificacionCasos)
    positivos = consultaGeograficaCOVIDPositivo(geografia)
    respuesta = mergeDics(clasificacion, positivos)
    respiradores = consultaGeograficaAsistenciaRespiratoria(geografia)
    respuesta = mergeDics(respuesta, respiradores)
    financiamiento = consultaGeograficaOrigenFinanciamiento(geografia)
    respuesta = mergeDics(respuesta, financiamiento)
    return respuesta

start_time = time.time()
# totales
clasificacionCasos = consultaTotales("clasificacion_resumen", "")
calculaTotales(clasificacionCasos)

departamentos = calculaZona('residencia_departamento_id', clasificacionCasos)
provincias = calculaZona('residencia_provincia_id', clasificacionCasos)

zonas = departamentos
zonas.update(provincias)

print("--- Tiempo parcial de ejecucion:  ",  divmod(time.time() - start_time,60) )

byIn1 = calculaByIn1(zonas, clasificacionCasos)

zonas = mergeDics(zonas, byIn1)

print(zonas)

persistir(file_zonas, zonas)

print("--- Tiempo de ejecucion:  ",  divmod(time.time() - start_time,60) )
