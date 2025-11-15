import cmd
import sys
from google.cloud import bigquery

class ArticleClassificationShell(cmd.Cmd):
    intro = 'Welcome to the Article Classification shell. Type help or ? to list commands.\n'
    prompt = '(article) '

    def __init__(self):
        super().__init__()
        self.client = bigquery.Client()
        self.table_id = "hexa-data-report-etl-prod.cdw_stage.drupal_article_classifications"

    def do_classify(self, arg):
        """Get classification for an article: classify <article_id>"""
        query = f"""
        SELECT classification
        FROM `{self.table_id}`
        WHERE article_id = '{arg}'
        LIMIT 1
        """
        query_job = self.client.query(query)
        results = query_job.result()
        for row in results:
            print(f"Article {arg} is classified as: {row['classification']}")

    def do_count(self, arg):
        """Count articles by classification: count <classification>"""
        query = f"""
        SELECT COUNT(*) as count
        FROM `{self.table_id}`
        WHERE classification = '{arg}'
        """
        query_job = self.client.query(query)
        results = query_job.result()
        for row in results:
            print(f"There are {row['count']} articles classified as '{arg}'")
            
    def do_count_all(self, arg):
        """Count articles for all user needs"""
        query = f"""
        SELECT classification, COUNT(*) as count
        FROM `{self.table_id}`
        GROUP BY classification
        ORDER BY count DESC
        """
        query_job = self.client.query(query)
        results = query_job.result()
        
        print("Article counts by classification:")
        for row in results:
            print(f"{row['classification']}: {row['count']} articles")

    def do_quit(self, arg):
        """Quit the program"""
        print("Thank you for using the Article Classification shell")
        return True
    
    def do_daily_breakdown(self, arg):
        """Get a daily breakdown of article classifications for the last 30 days"""
        query = f"""
        SELECT 
          DATE(timestamp) as date,
          classification, 
          COUNT(*) as count
        FROM `{self.table_id}`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        GROUP BY date, classification
        ORDER BY date DESC, count DESC
        """
        query_job = self.client.query(query)
        results = query_job.result()
        
        current_date = None
        for row in results:
            if row['date'] != current_date:
                if current_date:
                    print("--------------------")
                current_date = row['date']
                print(f"\nDate: {current_date}")
            print(f"  {row['classification']}: {row['count']} articles")

if __name__ == '__main__':
    ArticleClassificationShell().cmdloop()