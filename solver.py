from neo4j import GraphDatabase

URI      = "neo4j+s://7452e2ce.databases.neo4j.io"
USER     = "neo4j"
PASSWORD = "lcHisdIk814broKN5cAzsHR_aX9uLUdMea9ugBYhRWg"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def test_connection():
    
    driver.verify_connectivity()
    
    with driver.session() as session:
        result = session.run("RETURN 'Conexion exitosa a Neo4j' AS mensaje")
        print(result.single()["mensaje"])

class PuzzleSolver:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def solve_puzzle_from(self, puzzle_id, group_id, piece_id):
        with self.driver.session() as session:
            visited = set()
            instructions = []
            session.execute_read(
                self._dfs_from_piece,
                puzzle_id,
                group_id,
                piece_id,
                visited,
                instructions
            )
            return instructions

    @staticmethod
    def _dfs_from_piece(tx, puzzle_id, group_id, current_id, visited, instructions):
        key = (puzzle_id, group_id, current_id)
        if key in visited:
            return

        visited.add(key)

        result = tx.run("""
            MATCH (p:Piece {id: $current_id, groupId: $group_id, puzzleId: $puzzle_id})
            WHERE NOT p.isLost
            OPTIONAL MATCH (p)-[r:CONNECTS]->(n:Piece)
            WHERE NOT n.isLost
            RETURN n.id AS neighbor_id, r.direction AS direction
        """, current_id=current_id, group_id=group_id, puzzle_id=puzzle_id)

        for record in result:
            neighbor_id = record["neighbor_id"]
            direction = record["direction"]

            if neighbor_id is not None:
                neighbor_key = (puzzle_id, group_id, neighbor_id)
                if neighbor_key not in visited:
                    instructions.append(
                        f"Coloca la pieza {neighbor_id} en dirección {direction} desde la pieza {current_id}"
                    )
                    PuzzleSolver._dfs_from_piece(
                        tx, puzzle_id, group_id, neighbor_id, visited, instructions
                    )

# ----------------------------
if __name__ == "__main__":
    try:
        test_connection()

        solver = PuzzleSolver(URI, USER, PASSWORD)
        steps = solver.solve_puzzle_from(puzzle_id=2, group_id=1, piece_id=1)
        for step in steps:
            print(step)
        solver.close()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.close()
