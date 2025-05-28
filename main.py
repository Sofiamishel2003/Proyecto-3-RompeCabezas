
from neo4j import GraphDatabase
import json

URI      = "neo4j+s://7452e2ce.databases.neo4j.io"
USER     = "neo4j"
PASSWORD = "lcHisdIk814broKN5cAzsHR_aX9uLUdMea9ugBYhRWg"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def test_connection():
    
    driver.verify_connectivity()
    
    with driver.session() as session:
        result = session.run("RETURN 'Conexion exitosa a Neo4j' AS mensaje")
        print(result.single()["mensaje"])

# --- Clase para importar rompecabezas ---
class PuzzleImporter:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def import_puzzle(self, puzzle_json):
        with self.driver.session() as session:
            session.execute_write(self._create_puzzle_graph, puzzle_json)

    @staticmethod
    def _create_puzzle_graph(tx, puzzle_json):
        puzzle = puzzle_json["puzzle"]
        pid = puzzle["id"]
        pname = puzzle["name"]
        numPieces = puzzle["numPieces"]

        # Puzzle único
        tx.run("""
            MERGE (p:Puzzle {id: $pid})
            SET p.name = $name, p.numPieces = $numPieces
        """, pid=pid, name=pname, numPieces=numPieces)

        for group in puzzle["groups"]:
            gid = group["id"]

            # Group único por id + puzzleId
            tx.run("""
                MERGE (g:Group {id: $gid, puzzleId: $pid})
                WITH g
                MATCH (p:Puzzle {id: $pid})
                MERGE (g)-[:CONTAINED]->(p)
            """, gid=gid, pid=pid)

            # Piezas
            for piece in group["pieces"]:
                piece_id = piece["id"]
                isLost = piece["isLost"]

                tx.run("""
                    MERGE (pc:Piece {id: $pid, groupId: $gid, puzzleId: $puzid})
                    SET pc.isLost = $isLost
                    WITH pc
                    MATCH (g:Group {id: $gid, puzzleId: $puzid})
                    MERGE (pc)-[:BELONGS]->(g)
                """, pid=piece_id, gid=gid, puzid=pid, isLost=isLost)

            # Conexiones
            for piece in group["pieces"]:
                from_id = piece["id"]
                for conn in piece.get("connections", []):
                    to_id = conn["to"]
                    direction = conn["direction"]

                    tx.run("""
                        MATCH (a:Piece {id: $from_id, groupId: $gid, puzzleId: $pid}),
                            (b:Piece {id: $to_id, groupId: $gid, puzzleId: $pid})
                        MERGE (a)-[:CONNECTS {direction: $direction}]->(b)
                    """, from_id=from_id, to_id=to_id, gid=gid, pid=pid, direction=direction)

        # Relaciones LOCATED entre grupos (si hay más de uno)
        if len(puzzle["groups"]) > 1:
            for group in puzzle["groups"]:
                from_gid = group["id"]
                for loc in group.get("located", []):
                    to_gid = loc["to"]
                    direction = loc["direction"]
                    tx.run("""
                        MATCH (a:Group {id: $from_gid, puzzleId: $pid}),
                            (b:Group {id: $to_gid, puzzleId: $pid})
                        MERGE (a)-[:LOCATED {direction: $direction}]->(b)
                    """, from_gid=from_gid, to_gid=to_gid, pid=pid, direction=direction)



# --- Uso del script principal ---
if __name__ == "__main__":
    try:
        test_connection()

        # Cargar el JSON 
        with open("Rompecabezas2.json", "r", encoding="utf-8") as f:
            puzzle_data = json.load(f)

        importer = PuzzleImporter(URI, USER, PASSWORD)
        importer.import_puzzle(puzzle_data)
        print("Rompecabezas importado exitosamente.")
        importer.close()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.close()
