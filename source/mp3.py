from customer import Customer

import psycopg2

from config import read_config
from messages import *

POSTGRESQL_CONFIG_FILE_NAME = "database.cfg"

"""
    Connects to PostgreSQL database and returns connection object.
"""

# Mert AKÇA 2171163

def connect_to_db():
    db_conn_params = read_config(
        filename=POSTGRESQL_CONFIG_FILE_NAME, section="postgresql")
    conn = psycopg2.connect(**db_conn_params)
    conn.autocommit = False
    return conn


"""
    Splits given command string by spaces and trims each token.
    Returns token list.
"""


def tokenize_command(command):
    tokens = command.split(" ")
    return [t.strip() for t in tokens]


"""
    Prints list of available commands of the software.
"""


def help():
    # prints the choices for commands and parameters
    print("\n*** Please enter one of the following commands ***")
    print("> help")
    print("> sign_up <email> <password> <first_name> <last_name> <plan_id>")
    print("> sign_in <email> <password>")
    print("> sign_out")
    print("> show_plans")
    print("> show_subscription")
    print("> subscribe <plan_id>")
    print("> watched_movies <movie_id_1> <movie_id_2> <movie_id_3> ... <movie_id_n>")
    print("> search_for_movies <keyword_1> <keyword_2> <keyword_3> ... <keyword_n>")
    print("> suggest_movies")
    print("> quit")


"""
    Saves customer with given details.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def sign_up(conn, email, password, first_name, last_name, plan_id):
    cur = conn.cursor()
    try:
        cur.execute("select * from customer where email = %s;", (email,))
        user = cur.fetchone()
        if user != None:
            return False, CMD_EXECUTION_FAILED
        cur.execute("insert into customer (email, password, first_name, last_name, session_count, plan_id) values (%s, %s, %s, %s, %s, %s);",
                    (email, password, first_name, last_name, 0, plan_id))
        conn.commit()
        return True, CMD_EXECUTION_SUCCESS
    except:
        return False, CMD_EXECUTION_FAILED


"""
    Retrieves customer information if email and password is correct and customer's session_count < max_parallel_sessions.
    - Return type is a tuple, 1st element is a customer object and 2nd element is the response message from messages.py.
    - If email or password is wrong, return tuple (None, USER_SIGNIN_FAILED).
    - If session_count < max_parallel_sessions, commit changes (increment session_count) and return tuple (customer, CMD_EXECUTION_SUCCESS).
    - If session_count >= max_parallel_sessions, return tuple (None, USER_ALL_SESSIONS_ARE_USED).
    - If any exception occurs; rollback, do nothing on the database and return tuple (None, USER_SIGNIN_FAILED).
"""


def sign_in(conn, email, password):
    cur = conn.cursor()
    try:
        cur.execute(
            "select * from customer where email = %s and password = %s;", (email, password))
        user = cur.fetchone()
        if user == None:
            return None, USER_SIGNIN_FAILED
        customer = Customer(user[0], user[1], user[3],
                            user[4], user[5], user[6])
        cur.execute("select * from plan where plan_id = %s;",
                    (customer.plan_id,))
        plan = cur.fetchone()
        max_session = plan[3]
        if customer.session_count >= max_session:
            return None, USER_ALL_SESSIONS_ARE_USED
        cur.execute(
            "update customer set session_count = session_count + 1 where email = %s", (email,))
        conn.commit()
        return customer, CMD_EXECUTION_SUCCESS
    except:
        return None, CMD_EXECUTION_FAILED


"""
    Signs out from given customer's account.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Decrement session_count of the customer in the database.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def sign_out(conn, customer):
    cur = conn.cursor()
    try:
        cur.execute(
            "update customer set session_count = session_count - 1 where email = %s", (customer.email,))
        conn.commit()
        return True, CMD_EXECUTION_SUCCESS
    except:
        return False, CMD_EXECUTION_FAILED


"""
    Quits from program.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Remember to sign authenticated user out first.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def quit(conn, customer):
    try:
        if customer != None:
            sign_out(conn, customer)
        return True, CMD_EXECUTION_SUCCESS
    except:
        return False, CMD_EXECUTION_FAILED


"""
    Retrieves all available plans and prints them.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If the operation is successful; print available plans and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).

    Output should be like:
    #|Name|Resolution|Max Sessions|Monthly Fee
    1|Basic|720P|2|30
    2|Advanced|1080P|4|50
    3|Premium|4K|10|90
"""


def show_plans(conn):
    cur = conn.cursor()
    try:
        cur.execute("select * from plan")
        data = cur.fetchall()
        print("#|Name|Resolution|Max Sessions|Monthly Fee")
        for plan in data:
            print("|".join(map(str, plan)))
        return True, CMD_EXECUTION_SUCCESS
    except:
        return False, CMD_EXECUTION_FAILED


"""
    Retrieves authenticated user's plan and prints it. 
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If the operation is successful; print the authenticated customer's plan and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).

    Output should be like:
    #|Name|Resolution|Max Sessions|Monthly Fee
    1|Basic|720P|2|30
"""


def show_subscription(conn, customer):
    cur = conn.cursor()
    try:
        cur.execute("select * from plan where plan_id = %s;",
                    (customer.plan_id,))
        plan = cur.fetchone()
        print("#|Name|Resolution|Max Sessions|Monthly Fee")
        print("|".join(map(str, plan)))
        return True, CMD_EXECUTION_SUCCESS
    except:
        return False, CMD_EXECUTION_FAILED


"""
    Insert customer-movie relationships to Watched table if not exists in Watched table.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If a customer-movie relationship already exists, do nothing on the database and return (True, CMD_EXECUTION_SUCCESS).
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any one of the movie ids is incorrect; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def watched_movies(conn, customer, movie_ids):
    try:
        cur = conn.cursor()
        for mid in movie_ids:
            cur.execute("select * from movies where movie_id = %s",
                        (mid,))
            movie = cur.fetchone()
            if movie == None:
                return False, CMD_EXECUTION_FAILED
            cur.execute("select * from watched where customer_id = %s and movie_id = %s",
                        (customer.customer_id, mid))
            row = cur.fetchone()
            if row == None:
                cur.execute(
                    "insert into watched (customer_id, movie_id) values (%s, %s)", (customer.customer_id, mid))
        conn.commit()
        return True, CMD_EXECUTION_SUCCESS
    except:
        return False, CMD_EXECUTION_FAILED


"""
    Subscribe authenticated customer to new plan.
    - Return type is a tuple, 1st element is a customer object and 2nd element is the response message from messages.py.
    - If target plan does not exist on the database, return tuple (None, SUBSCRIBE_PLAN_NOT_FOUND).
    - If the new plan's max_parallel_sessions < current plan's max_parallel_sessions, return tuple (None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE).
    - If the operation is successful, commit changes and return tuple (customer, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (None, CMD_EXECUTION_FAILED).
"""


def subscribe(conn, customer, plan_id):
    try:
        cur = conn.cursor()
        cur.execute("select * from plan where plan_id = %s;", (plan_id,))
        plan = cur.fetchone()
        if plan == None:
            return None, SUBSCRIBE_PLAN_NOT_FOUND
        cur.execute("select * from plan where plan_id = %s;",
                    (customer.plan_id,))
        current_plan = cur.fetchone()
        if plan[3] < current_plan[3]:
            return None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE
        cur.execute("update customer set plan_id = %s where customer_id = %s",
                    (plan_id, customer.customer_id))
        customer.plan_id = plan_id
        conn.commit()
        return customer, CMD_EXECUTION_SUCCESS
    except:
        return None, CMD_EXECUTION_FAILED


"""
    Searches for movies with given search_text.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Print all movies whose titles contain given search_text IN CASE-INSENSITIVE MANNER.
    - If the operation is successful; print movies found and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).

    Output should be like:
    Id|Title|Year|Rating|Votes|Watched
    "tt0147505"|"Sinbad: The Battle of the Dark Knights"|1998|2.2|149|0
    "tt0468569"|"The Dark Knight"|2008|9.0|2021237|1
    "tt1345836"|"The Dark Knight Rises"|2012|8.4|1362116|0
    "tt3153806"|"Masterpiece: Frank Millers The Dark Knight Returns"|2013|7.8|28|0
    "tt4430982"|"Batman: The Dark Knight Beyond"|0|0.0|0|0
    "tt4494606"|"The Dark Knight: Not So Serious"|2009|0.0|0|0
    "tt4498364"|"The Dark Knight: Knightfall - Part One"|2014|0.0|0|0
    "tt4504426"|"The Dark Knight: Knightfall - Part Two"|2014|0.0|0|0
    "tt4504908"|"The Dark Knight: Knightfall - Part Three"|2014|0.0|0|0
    "tt4653714"|"The Dark Knight Falls"|2015|5.4|8|0
    "tt6274696"|"The Dark Knight Returns: An Epic Fan Film"|2016|6.7|38|0
"""


def search_for_movies(conn, customer, search_text):
    try:
        cur = conn.cursor()
        cur.execute(
            "select *, (CASE WHEN (exists(select * from watched w where w.customer_id = %s and w.movie_id = m.movie_id)) THEN 1 ELSE 0 END) as watched from movies m where lower(m.title) like lower(%s) order by m.movie_id", (customer.customer_id, '%' + search_text + '%',))
        movies = cur.fetchall()
        print("Id|Title|Year|Rating|Votes|Watched")
        if movies != None and movies != []:
            for movie in movies:
                print("|".join(map(str, movie)))
        return True, CMD_EXECUTION_SUCCESS
    except:
        return False, CMD_EXECUTION_FAILED


"""
    Suggests combination of these movies:
        1- Find customer's genres. For each genre, find movies with most number of votes among the movies that the customer didn't watch.

        2- Find top 10 movies with most number of votes and highest rating, such that these movies are released 
           after 2010 ( [2010, today) ) and the customer didn't watch these movies.
           (descending order for votes, descending order for rating)

        3- Find top 10 movies with votes higher than the average number of votes of movies that the customer watched.
           Disregard the movies that the customer didn't watch.
           (descending order for votes)

    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.    
    - Output format and return format are same with search_for_movies.
    - Order these movies by their movie id, in ascending order at the end.
    - If the operation is successful; print movies suggested and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
"""


def suggest_movies(conn, customer):
    try:
        cur = conn.cursor()
        cur.execute("""
                select distinct tb.movie_id, tb.title, tb.movie_year, tb.rating, tb.votes
        from ((
            select
                m.movie_id,
                m.title,
                m.movie_year,
                m.rating,
                m.votes
            from
                (
                    select
                        gn.genre_name,
                        max(m.votes) as max_vote
                    from
                        (
                            select
                                distinct g.genre_name
                            from
                                watched w,
                                genres g
                            where
                                w.customer_id = %(cid)s
                                and w.movie_id = g.movie_id
                        ) as gn,
                        (
                            select
                                *
                            from
                                movies m
                                left join watched w on m.movie_id = w.movie_id
                                and w.customer_id = %(cid)s,
                                genres g
                            where
                                w.movie_id is null
                                and g.movie_id = m.movie_id
                        ) as m
                    where
                        gn.genre_name = m.genre_name
                    group by
                        gn.genre_name
                ) as mv,
                movies m
            where
                m.votes = mv.max_vote
        )
        union
        (
            select
                m.movie_id,
                m.title,
                m.movie_year,
                m.rating,
                m.votes
            from
                (
                    select
                        m.movie_id,
                m.title,
                m.movie_year,
                m.rating,
                m.votes
                    from
                        movies m
                        left join watched w on m.movie_id = w.movie_id
                        and w.customer_id = %(cid)s
                    where
                        w.movie_id is null
                ) as m
            where
                m.movie_year >= 2010
            order by
                m.votes desc,
                m.rating desc
            limit
                10
        )
        union
        (
            select
                m.movie_id,
                m.title,
                m.movie_year,
                m.rating,
                m.votes
            from
                movies m
                left join watched w on m.movie_id = w.movie_id
                and w.customer_id = %(cid)s
            where
                w.movie_id is null
                and m.votes > (
                    select
                        avg(m.votes)
                    from
                        movies m,
                        watched w
                    where
                        w.movie_id = m.movie_id
                        and w.customer_id = %(cid)s
                )
            order by
                m.votes desc
            limit
                10
        )) as tb
        order by tb.movie_id
        """, {"cid": customer.customer_id})
        movies = cur.fetchall()
        print("Id|Title|Year|Rating|Votes")
        if movies != None and movies != []:
            for movie in movies:
                print("|".join(map(str, movie)))
        return True, CMD_EXECUTION_SUCCESS
    except:
        return False, CMD_EXECUTION_FAILED
