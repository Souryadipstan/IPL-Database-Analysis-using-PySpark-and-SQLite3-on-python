import sqlite3
import pandas as pd

class Database:

    df = pd.read_csv('/content/ipl_ball_by_ball.csv')
    df1 =  pd.read_csv('/content/ipl_matches.csv')
    df2 = pd.read_csv('/content/ipl_venue.csv')

    DB_LOCATION = 'ipldb.db'

    def __init__(self):
        self.connection = sqlite3.connect(Database.DB_LOCATION)
        self.cur = self.connection.cursor()

        Database.df.to_sql("balls", self.connection, if_exists='replace', index=False)
        Database.df1.to_sql("matches", self.connection, if_exists='replace', index=False)
        Database.df2.to_sql("venue", self.connection, if_exists='replace', index=False)
        
    def execute(self, sql_query):
        self.cur.execute(sql_query)

    def get_query1_result(self):
        self.cur.execute("""
                            select b.venue,count(b.venue_id) as num_matches from matches a
                            inner join venue b on a.venue_id=b.venue_id
                            where a.eliminator = 'Y'
                            group by b.venue
                            order by 2 desc;
                        """
        )
        rows = self.cur.fetchall()
        for row in rows:
            print(row)

    def get_query2_result(self):
        self.cur.execute(
            """
            select fielder,count(dismissal_kind) as number_of_catches
            from balls
            where dismissal_kind = 'caught'
            group by fielder
            order by count(dismissal_kind) desc
            """
        )
        rows = self.cur.fetchall()
        for row in rows:
            print(row)

    def get_query3_result(self):
        self.cur.execute("""
        select a.bowler,count(a.is_wicket) as number_of_wickets
        from balls a, matches b
        where a.match_id = b.match_id and b.method == 'D/L' and a.is_wicket = 1
        group by bowler
        order by number_of_wickets desc
        """)
        rows = self.cur.fetchall()
        for row in rows:
            print(row)

    def get_query4_result(self):
        self.cur.execute("""
        select batsman, (sum(batsman_runs)/count(batsman))*100 as strike_rate
        from balls
        where overs between 7 and 20
        group by batsman
        order by strike_rate desc
        """)
        rows = self.cur.fetchall()
        for row in rows:
            print(row)
        
    def get_query5_result(self):
        self.cur.execute("""
        select c.venue,c.city, sum(a.extra_runs) as total_extra_runs
        from balls a
        join matches b on a.match_id = b.match_id
        join venue c on b.venue_id = c.venue_id
        group by c.venue,c.city
        order by total_extra_runs desc
        """)
        rows = self.cur.fetchall()
        for row in rows:
            print(row)

    def get_query6_result(self):
        self.cur.execute("""
        select b.player_of_match,count(b.player_of_match) as Number_of_player_of_match
        from matches b, venue c 
        where b.neutral_venue = 1 and c.venue_id = b.venue_id
        group by b.player_of_match
        order by Number_of_player_of_match desc
        """)
        rows = self.cur.fetchall()
        for row in rows:
            print(row)
        
    def get_query7_result(self):
        self.cur.execute("""
        select batsman, (sum(batsman_runs)/sum(is_wicket)) as batting_average
        from balls
        group by batsman
        order by batting_average desc
        """)
        rows = self.cur.fetchall()
        for row in rows:
            print(row)

    def get_query8_result(self):
        self.cur.execute("""
        select umpire1, count(umpire1) as num_times_officiated
        from matches
        group by umpire1
        order by num_times_officiated desc
        limit 1
        """)
        rows = self.cur.fetchall()
        for row in rows:
            print(row)
        
    def get_query9_result(self):
        self.cur.execute("""
        select b.match_id,c.venue,c.city,sum(a.batsman_runs)
        from balls a
        join matches b on a.match_id=b.match_id
        join venue c on b.venue_id =c.venue_id
        where a.batsman= 'V Kohli'
        group by b.match_id
        order by sum(a.batsman_runs) desc limit 1; 
        """)
        rows = self.cur.fetchall()
        for row in rows:
            print(row)

    def get_query10_result(self):
        self.cur.execute("""
        select 'Won when toss won' as Win_status,count(winner) as Winner
        from matches
        where winner = toss_winner

        UNION 

        select 'Won when toss lost' as Win_status,count(winner) as Winner
        from matches
        where winner <> toss_winner
        """)
        rows = self.cur.fetchall()
        for row in rows:
            print(row)

    def get_status(self):
     try:
        self.cur
        return "Database connected"
     except Exception as e:
        return "Database not connected"

    def close(self):
        self.cur.close()


