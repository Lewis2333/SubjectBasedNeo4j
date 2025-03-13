from neo4j import GraphDatabase, exceptions
import csv
import os


class Neo4jImporter:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def import_data(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not exist!")

        with self.driver.session() as session:
            try:
                # 创建主节点
                self.execute_transaction_with_retry(session, self._create_main_node)

                with open(file_path, 'r', encoding='utf-8') as file:
                    reader = csv.reader(file)
                    # 跳过表头
                    next(reader)

                    for row in reader:
                        unit = row[0] if len(row) > 0 else ""
                        chapter = row[1] if len(row) > 1 else ""
                        section = row[2] if len(row) > 2 else ""

                        self.execute_transaction_with_retry(session, self._create_unit_node, unit)
                        self.execute_transaction_with_retry(session, self._create_chapter_node, chapter)
                        self.execute_transaction_with_retry(session, self._create_section_node, section)

                        self.execute_transaction_with_retry(session, self._create_include_relation, unit)
                        self.execute_transaction_with_retry(session, self._create_has_chapter_relation, unit, chapter)
                        self.execute_transaction_with_retry(session, self._create_has_section_relation, chapter, section)
            except Exception as e:
                print(f"error: {e}")

    def execute_transaction_with_retry(self, session, transaction_function, *args):
        retry = False
        try:
            session.execute_write(transaction_function, *args)
        except exceptions.TransactionError as e:
            if "InvalidBookmark" in str(e):
                retry = True
            else:
                raise e

        if retry:
            # 不传递书签，重新尝试执行事务
            with self.driver.session(bookmarks=None) as new_session:
                new_session.execute_write(transaction_function, *args)

    @staticmethod
    def _create_main_node(tx):
        tx.run("MERGE (m:Main {name: 'test'})")

    @staticmethod
    def _create_unit_node(tx, unit):
        tx.run("MERGE (u:Unit {name: $unit})", unit=unit)

    @staticmethod
    def _create_chapter_node(tx, chapter):
        tx.run("MERGE (c:Chapter {name: $chapter})", chapter=chapter)

    @staticmethod
    def _create_section_node(tx, section):
        tx.run("MERGE (s:Section {name: $section})", section=section)

    @staticmethod
    def _create_include_relation(tx, unit):
        tx.run("MATCH (m:Main {name: 'test'}), (u:Unit {name: $unit}) "
               "MERGE (m)-[:include]->(u)", unit=unit)

    @staticmethod
    def _create_has_chapter_relation(tx, unit, chapter):
        tx.run("MATCH (u:Unit {name: $unit}), (c:Chapter {name: $chapter}) "
               "MERGE (u)-[:has_chapter]->(c)", unit=unit, chapter=chapter)

    @staticmethod
    def _create_has_section_relation(tx, chapter, section):
        tx.run("MATCH (c:Chapter {name: $chapter}), (s:Section {name: $section}) "
               "MERGE (c)-[:has_section]->(s)", chapter=chapter, section=section)

if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "*******"
    file_path = "test.csv"

    try:
        importer = Neo4jImporter(uri, user, password)
        importer.import_data(file_path)
        importer.close()
    except Exception as e:
        print(f"Running wrong: {e}")