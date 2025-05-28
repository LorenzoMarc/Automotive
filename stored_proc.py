import psycopg2


def ro_sessions():

    try:
        ps_connection = set_connection()

        ps_connection.autocommit = True
        cursor = ps_connection.cursor()
        print("PostgreSQL connection is open")
        print("Calculating RO sessions")
        cursor.execute('CALL "FcaData".calculate_ro_sessions()')
        print("Computation ended")
        cursor.execute('TRUNCATE TABLE "FcaData".ro_log')
        if ps_connection:
            cursor.close()
            ps_connection.close()
            print("PostgreSQL connection is closed")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
        return False

    return True


def phev_sessions():

    try:
        ps_connection = set_connection()

        ps_connection.autocommit = True
        cursor = ps_connection.cursor()
        print("PostgreSQL connection is open")
        print("Calculating PHEV sessions")
        cursor.execute('CALL "FcaData".calculate_phev_session()')
        print("Computation ended")
        cursor.execute('TRUNCATE TABLE "FcaData".phev_log')
        if ps_connection:
            cursor.close()
            ps_connection.close()
            print("PostgreSQL connection is closed")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
        return False

    return True


def set_connection ():
    conn = psycopg2.connect(user="postgres",
                     password="admin",
                     host="localhost",
                     port="5432",
                     database='postgres')
    return conn


def vf_sessions():
    try:
        ps_connection = set_connection()

        ps_connection.autocommit = True
        cursor = ps_connection.cursor()
        print("PostgreSQL connection is open")
        print("Calculating VF sessions")
        cursor.execute('CALL "FcaData".calculate_vf_session()')
        print("Computation ended")
        cursor.execute('TRUNCATE TABLE "FcaData".vf_log')
        if ps_connection:
            cursor.close()
            ps_connection.close()
            print("PostgreSQL connection is closed")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
        return False

    return True
