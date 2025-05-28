
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

        # Crear el puzzle
        tx.run("""
            MERGE (p:Puzzle {id: $pid})
            SET p.name = $name, p.numPieces = $numPieces
        """, pid=pid, name=pname, numPieces=numPieces)

        for group in puzzle["groups"]:
            gid = group["id"]

            # Crear grupo con ID contextual (temporalmente con puzzleId)
            tx.run("""
                MERGE (g:Group {id: $gid, puzzleId: $pid})
                WITH g
                MATCH (p:Puzzle {id: $pid})
                MERGE (g)-[:CONTAINED]->(p)
            """, gid=gid, pid=pid)

            # Crear piezas con contexto de puzzle y grupo (temporalmente)
            for piece in group["pieces"]:
                piece_id = piece["id"]
                isLost = piece["isLost"]

                tx.run("""
                    MERGE (pc:Piece {id: $piece_id, groupId: $gid, puzzleId: $pid})
                    SET pc.isLost = $isLost
                    WITH pc
                    MATCH (g:Group {id: $gid, puzzleId: $pid})
                    MERGE (pc)-[:BELONGS]->(g)
                """, piece_id=piece_id, gid=gid, pid=pid, isLost=isLost)

            # Conexiones entre piezas del mismo grupo
            for piece in group["pieces"]:
                from_id = piece["id"]
                for conn in piece.get("connections", []):
                    to_id = conn["to"]
                    direction = conn["direction"]

                    tx.run("""
                        MATCH 
                            (a:Piece {id: $from_id, groupId: $gid, puzzleId: $pid}),
                            (b:Piece {id: $to_id, groupId: $gid, puzzleId: $pid})
                        MERGE (a)-[:CONNECTS {direction: $direction}]->(b)
                    """, from_id=from_id, to_id=to_id, gid=gid, pid=pid, direction=direction)

        # Relaciones LOCATED entre grupos del mismo puzzle
        for group in puzzle["groups"]:
            from_gid = group["id"]
            for loc in group.get("located", []):
                to_gid = loc["to"]
                direction = loc["direction"]

                tx.run("""
                    MATCH 
                        (a:Group {id: $from_gid, puzzleId: $pid}),
                        (b:Group {id: $to_gid, puzzleId: $pid})
                    MERGE (a)-[:LOCATED {direction: $direction}]->(b)
                """, from_gid=from_gid, to_gid=to_gid, pid=pid, direction=direction)

        #  Limpiar propiedades temporales
        tx.run("MATCH (g:Group {puzzleId: $pid}) REMOVE g.puzzleId", pid=pid)
        tx.run("MATCH (pc:Piece {puzzleId: $pid}) REMOVE pc.puzzleId, pc.groupId", pid=pid)

def update_piece_is_lost(puzzle_id, group_id, piece_id, is_lost):
    with driver.session() as session:
        result = session.run("""
            MATCH (pc:Piece {id: $piece_id})
            MATCH (pc)-[:BELONGS]->(g:Group {id: $group_id})-[:CONTAINED]->(p:Puzzle {id: $puzzle_id})
            SET pc.isLost = $is_lost
            RETURN pc.id AS id, pc.isLost AS isLost
        """, piece_id=piece_id, group_id=group_id, puzzle_id=puzzle_id, is_lost=is_lost)
        
        record = result.single()
        if record:
            print(f"Pieza {record['id']} actualizada: isLost = {record['isLost']}")
        else:
            print("No se encontró la pieza para actualizar.")

# --- Uso del script principal ---
if __name__ == "__main__":
    try:
        test_connection()

        # Cargar el JSON 
        with open("Rompecabezas2.json", "r", encoding="utf-8") as f:
            puzzle_data = json.load(f)

        importer = PuzzleImporter(URI, USER, PASSWORD)
        importer.import_puzzle(puzzle_data)
        print("Rompecabezas 2 importado exitosamente.")
        # Cargar el JSON 2
        with open("Rompecabezas1.json", "r", encoding="utf-8") as f:
            puzzle_data = json.load(f)

        importer = PuzzleImporter(URI, USER, PASSWORD)
        importer.import_puzzle(puzzle_data)
        print("Rompecabezas 1 importado exitosamente.")
        importer.close()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.close()

"""

if __name__ == "__main__":
    try:
        test_connection()

        # Cambiar isLost de una pieza (ejemplo)
        update_piece_is_lost(puzzle_id=1, group_id=1, piece_id=3, is_lost=True)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.close()

"""

