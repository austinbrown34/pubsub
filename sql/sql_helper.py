import os
import config.settings as settings

dir_path = os.path.join(settings.APPLICATION_PATH, settings.SQL_PATH)


def execute_sql_file(db, filename, params, fetchall=False):
    with open(dir_path + '/' + filename, 'r') as f:
        query = f.read()
        method = db.execute_and_fetchall if fetchall else db.execute_and_fetchone
        return method(
            query,
            params
        )


# Have to do this because if we use the param fields it doesn't case the datetime correctly
def execute_sql_file_with_formatting(db, filename, params, fetchall=False):
    """
    Helper method for executing sql command where you want to string replacement as opposed
    to using pytds params functionality
    """
    with open(dir_path + '/' + filename, 'r') as f:
        query = f.read()
        method = db.execute_and_fetchall if fetchall else db.execute_and_fetchone

        for param in params:
            query = query.replace(param, params[param])
        return method(query)
