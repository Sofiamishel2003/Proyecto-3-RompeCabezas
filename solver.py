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
            visited_pieces = set()
            visited_groups = set()
            instructions = []
            missing_pieces = []
            # Resuelve a partir del punto inicial
            session.execute_read(
                self._resolve_group_recursive,
                puzzle_id,
                group_id,
                piece_id,
                visited_groups,
                visited_pieces,
                instructions,
                missing_pieces
            )
            # Revisa si hay otros grupos no visitados
            extra_groups = session.run("""
                MATCH (p:Puzzle {id: $puzzle_id})<-[:CONTAINED]-(g:Group)
                RETURN g.id AS group_id
            """, puzzle_id=puzzle_id)

            for record in extra_groups:
                gid = record["group_id"]
                if (puzzle_id, gid) not in visited_groups:
                    # Obtener una pieza válida del grupo no visitado
                    piece_result = session.run("""
                        MATCH (g:Group {id: $gid})<-[:BELONGS]-(pc:Piece)
                        WHERE NOT pc.isLost
                        RETURN pc.id AS pid
                        LIMIT 1
                    """, gid=gid)

                    piece_rec = piece_result.single()
                    if piece_rec:
                        pid = piece_rec["pid"]
                        instructions.append(f"Grupo {gid} no estaba conectado. Empezando desde pieza {pid}.")
                        session.execute_read(
                            self._resolve_group_recursive,
                            puzzle_id,
                            gid,
                            pid,
                            visited_groups,
                            visited_pieces,
                            instructions,
                            missing_pieces
                        )

            # Lista de piezas perdidas
            if missing_pieces:
                instructions.append("Piezas perdidas detectadas:")
                for mp in missing_pieces:
                    instructions.append(f"Pieza {mp[0]} - Grupo {mp[1]}")

            instructions.append("RompeCabezas resuelto.")
            return instructions

    @staticmethod
    def _resolve_group_recursive(tx, puzzle_id, group_id, piece_id, visited_groups, visited_pieces, instructions, missing_pieces):
        if (puzzle_id, group_id) in visited_groups:
            return

        visited_groups.add((puzzle_id, group_id))

        # DFS del grupo actual
        PuzzleSolver._dfs_group_pieces(
            tx, puzzle_id, group_id, piece_id, visited_pieces, instructions, visited_edges=set(),missing_pieces=missing_pieces
        )
        # Verificar piezas perdidas no exploradas
        lost_pieces_result = tx.run("""
            MATCH (pz:Puzzle {id: $puzzle_id})<-[:CONTAINED]-(g:Group {id: $group_id})<-[:BELONGS]-(pc:Piece)
            WHERE pc.isLost
            RETURN pc.id AS lost_id
        """, puzzle_id=puzzle_id, group_id=group_id)

        for record in lost_pieces_result:
            lost_id = record["lost_id"]
            key = (puzzle_id, group_id, lost_id)
            if key not in visited_pieces:
                missing_pieces.append((lost_id, group_id))
        # Detectar conexiones a piezas perdidas desde piezas exploradas
        extra_conns = tx.run("""
            MATCH (p1:Piece)-[r:CONNECTS]->(p2:Piece)
            WHERE (p1)-[:BELONGS]->(:Group {id: $group_id}) 
              AND (p2)-[:BELONGS]->(:Group {id: $group_id}) 
              AND NOT p1.isLost AND p2.isLost
            RETURN p1.id AS from_id, p2.id AS to_id, r.direction AS direction
        """, group_id=group_id)

        for record in extra_conns:
            from_id = record["from_id"]
            to_id = record["to_id"]
            direction = record["direction"]
            key_from = (puzzle_id, group_id, from_id)
            key_to = (puzzle_id, group_id, to_id)

            if key_from in visited_pieces and key_to not in visited_pieces:
                instructions.append(
                    f"(Conexión faltante) Coloca la pieza {to_id}-{group_id} en dirección {direction} de la pieza {from_id}-{group_id} (pieza perdida)"
                )
                # Verificar piezas no exploradas que están conectadas solo a piezas perdidas o no conectadas a nadie
        unvisited_result = tx.run("""
            MATCH (g:Group {id: $group_id})<-[:BELONGS]-(p:Piece)
            WHERE NOT p.isLost
            RETURN p.id AS pid
        """, group_id=group_id)

        for record in unvisited_result:
            pid = record["pid"]
            key = (puzzle_id, group_id, pid)
            if key not in visited_pieces:
                # ¿Tiene alguna conexión válida?
                conn_check = tx.run("""
                    OPTIONAL MATCH (p:Piece {id: $pid})-[:CONNECTS]->(o1:Piece)
                    WHERE (o1)-[:BELONGS]->(:Group {id: $group_id}) AND NOT o1.isLost

                    OPTIONAL MATCH (o2:Piece)-[:CONNECTS]->(p:Piece {id: $pid})
                    WHERE (o2)-[:BELONGS]->(:Group {id: $group_id}) AND NOT o2.isLost

                    RETURN COUNT(DISTINCT o1) + COUNT(DISTINCT o2) AS valid_connections
                """, pid=pid, group_id=group_id)

                if conn_check.single()["valid_connections"] == 0:
                    instructions.append(
                        f"Pieza {pid}-{group_id} no pudo colocarse porque está aislada o solo conectada a piezas perdidas."
                    )

        # Detectar piezas no exploradas que están conectadas HACIA piezas perdidas
        inferred_from_lost = tx.run("""
            MATCH (from:Piece)-[r:CONNECTS]->(to:Piece)
            WHERE (from)-[:BELONGS]->(:Group {id: $group_id})
              AND (to)-[:BELONGS]->(:Group {id: $group_id})
              AND from.isLost = false AND to.isLost = true
            RETURN from.id AS pid, to.id AS lost_id, r.direction AS dir
        """, group_id=group_id)

        for record in inferred_from_lost:
            pid = record["pid"]
            lost_id = record["lost_id"]
            direction = record["dir"]
            key = (puzzle_id, group_id, pid)

            if key not in visited_pieces:
                # Invertir dirección
                inverted = {
                    "Up": "Down",
                    "Down": "Up",
                    "Left": "Right",
                    "Right": "Left"
                }.get(direction, f"(inverso de {direction})")
                
                instructions.append(
                    f"(Inferida) Coloca la pieza {pid}-{group_id} en dirección {inverted} de la pieza perdida {lost_id}-{group_id}"
                )
                visited_pieces.add(key)

        # Anotar grupo terminado
        instructions.append(f"Grupo {group_id} terminado.")
        # Buscar grupos adyacentes por LOCATED dentro del mismo puzzle
        result = tx.run("""
            MATCH (p:Puzzle {id: $puzzle_id})<-[:CONTAINED]-(g1:Group {id: $group_id})-[r:LOCATED]->(g2:Group)-[:CONTAINED]->(p)
            RETURN DISTINCT g1.id AS from_gid, g2.id AS to_gid, r.direction AS direction
        """, puzzle_id=puzzle_id, group_id=group_id)

        for record in result:
            from_gid = record["from_gid"]
            to_gid = record["to_gid"]
            direction = record["direction"]
            instructions.append(f"Coloca el grupo {to_gid} a la {direction.lower()} del grupo {from_gid}.")

            # Obtener una pieza válida (no perdida) en ese grupo
            piece_result = tx.run("""
                MATCH (pz:Puzzle {id: $puzzle_id})<-[:CONTAINED]-(g:Group {id: $group_id})<-[:BELONGS]-(pc:Piece)
                WHERE NOT pc.isLost
                RETURN pc.id AS start_piece_id
                LIMIT 1
            """, puzzle_id=puzzle_id, group_id=to_gid)

            piece_rec = piece_result.single()
            if piece_rec:
                start_piece = piece_rec["start_piece_id"]
                PuzzleSolver._resolve_group_recursive(
                    tx, puzzle_id, to_gid, start_piece, visited_groups, visited_pieces, instructions,missing_pieces
                )

    @staticmethod
    def _dfs_group_pieces(tx, puzzle_id, group_id, current_id, visited_pieces, instructions, visited_edges=None, missing_pieces=None):
        if visited_edges is None:
            visited_edges = set()

        key = (puzzle_id, group_id, current_id)
        if key in visited_pieces:
            return

        visited_pieces.add(key)


        # Obtener conexiones salientes y entrantes
        result = tx.run("""
            MATCH (pz:Puzzle {id: $puzzle_id})<-[:CONTAINED]-(g:Group {id: $group_id})
            MATCH (g)<-[:BELONGS]-(p:Piece {id: $current_id})
            WHERE NOT p.isLost

            OPTIONAL MATCH (p)-[r1:CONNECTS]->(n1:Piece)
            WHERE NOT n1.isLost AND (n1)-[:BELONGS]->(g) AND n1.id <> p.id

            OPTIONAL MATCH (n2:Piece)-[r2:CONNECTS]->(p)
            WHERE NOT n2.isLost AND (n2)-[:BELONGS]->(g) AND n2.id <> p.id

            WITH p, 
                COLLECT(DISTINCT {id: n1.id, direction: r1.direction, from: p.id, to: n1.id}) + 
                COLLECT(DISTINCT {id: n2.id, direction: r2.direction, from: n2.id, to: p.id}) AS neighbors
            RETURN neighbors
        """, puzzle_id=puzzle_id, group_id=group_id, current_id=current_id)

        records = result.single()
        neighbors = records["neighbors"] if records else []

        for neighbor in neighbors:
            neighbor_id = neighbor["id"]
            direction = neighbor["direction"]
            from_id = neighbor["from"]
            to_id = neighbor["to"]

            edge_key = (from_id, to_id)

            if neighbor_id and edge_key not in visited_edges:
                visited_edges.add(edge_key)
                instructions.append(
                    f"Coloca la pieza {to_id}-{group_id} en dirección {direction} de la pieza {from_id}-{group_id}"
                )
                PuzzleSolver._dfs_group_pieces(
                    tx, puzzle_id, group_id, neighbor_id, visited_pieces, instructions, visited_edges
                )

# ----------------------------
if __name__ == "__main__":
    try:
        test_connection()

        solver = PuzzleSolver(URI, USER, PASSWORD)
        steps = solver.solve_puzzle_from(puzzle_id=3, group_id=1, piece_id=1)
        print("Importante: Todas las instrucciones de ensamblaje asumen que los números en las piezas están orientados correctamente, es decir, en la posición normal de lectura.")
        print("Asegúrate de colocar cada pieza manteniendo esta orientación, ya que las direcciones como arriba, abajo, izquierda o derecha se basan en ella.")
        for step in steps:
            print(step)
        solver.close()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.close()
