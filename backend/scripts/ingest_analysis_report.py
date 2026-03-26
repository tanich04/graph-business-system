import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


sys.path.append(str(Path(__file__).parent.parent))


SCRIPT_DIR = Path(__file__).resolve().parent
REPORT_PATH = SCRIPT_DIR / "analyze_data_report.txt"
SAFE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def is_safe_neo4j_name(value: str) -> bool:
    return bool(SAFE_NAME_PATTERN.fullmatch(value))


class AnalysisReportParser:
    def __init__(self, report_path: Path):
        self.report_path = report_path

    def parse(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        lines = self.report_path.read_text(encoding="utf-8").splitlines()
        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []

        index = 0
        while index < len(lines):
            line = lines[index].strip()

            if line == "Nodes:":
                payload, index = self._consume_json_block(lines, index + 1)
                if isinstance(payload, list):
                    nodes.extend(payload)
                continue

            if line == "Edges:":
                payload, index = self._consume_json_block(lines, index + 1)
                if isinstance(payload, list):
                    edges.extend(payload)
                continue

            index += 1

        return nodes, edges

    def _consume_json_block(self, lines: List[str], start_index: int) -> Tuple[Any, int]:
        while start_index < len(lines) and not lines[start_index].strip():
            start_index += 1

        if start_index >= len(lines):
            return None, start_index

        opening = lines[start_index].lstrip()
        if not opening.startswith("{") and not opening.startswith("["):
            return None, start_index

        json_lines: List[str] = []
        brace_balance = 0
        bracket_balance = 0
        started = False
        index = start_index

        while index < len(lines):
            raw_line = lines[index]
            stripped = raw_line.lstrip()

            if not started and not stripped:
                index += 1
                continue

            started = True
            json_lines.append(stripped)
            brace_balance += stripped.count("{") - stripped.count("}")
            bracket_balance += stripped.count("[") - stripped.count("]")
            index += 1

            if brace_balance == 0 and bracket_balance == 0:
                break

        payload = json.loads("\n".join(json_lines))
        return payload, index


class AnalysisReportIngestor:
    def __init__(self, report_path: Path):
        self.report_path = report_path
        self.driver = None
        self.neo4j_client = None

    async def ingest(self) -> None:
        from app.database.neo4j_client import neo4j_client

        self.neo4j_client = neo4j_client
        parser = AnalysisReportParser(self.report_path)
        nodes, edges = parser.parse()

        print(f"Loaded {len(nodes)} nodes and {len(edges)} edges from {self.report_path}")

        await self.neo4j_client.connect()
        self.driver = self.neo4j_client.driver

        async with self.driver.session() as session:
            await self.create_id_constraints(session, nodes, edges)
            await self.upsert_nodes(session, nodes, edges)
            await self.upsert_edges(session, edges)
            await self.print_summary(session)

        await self.neo4j_client.close()

    async def create_id_constraints(
        self,
        session,
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
    ) -> None:
        labels = {
            node["label"]
            for node in nodes
            if isinstance(node, dict) and is_safe_neo4j_name(node.get("label", ""))
        }

        for edge in edges:
            for endpoint_key in ("from", "to"):
                endpoint = edge.get(endpoint_key, {})
                label = endpoint.get("label")
                if is_safe_neo4j_name(label):
                    labels.add(label)

        for label in sorted(labels):
            query = f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE"
            await session.run(query)

    async def upsert_nodes(
        self,
        session,
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
    ) -> None:
        nodes_by_label: Dict[str, Dict[str, Dict[str, Any]]] = {}

        for node in nodes:
            label = node.get("label")
            node_id = node.get("id")
            if not is_safe_neo4j_name(label) or not node_id:
                continue

            properties = dict(node.get("properties", {}))
            properties["id"] = str(node_id)
            nodes_by_label.setdefault(label, {})[str(node_id)] = properties

        # Ensure relationship endpoints exist even if the report has no explicit node block for them.
        for edge in edges:
            for endpoint_key in ("from", "to"):
                endpoint = edge.get(endpoint_key, {})
                label = endpoint.get("label")
                node_id = endpoint.get("id")
                if not is_safe_neo4j_name(label) or not node_id:
                    continue
                nodes_by_label.setdefault(label, {}).setdefault(str(node_id), {"id": str(node_id)})

        for label, node_map in nodes_by_label.items():
            records = [{"id": node_id, "properties": properties} for node_id, properties in node_map.items()]
            query = f"""
            UNWIND $records AS record
            MERGE (n:{label} {{id: record.id}})
            SET n += record.properties
            """
            await session.run(query, records=records)
            print(f"Upserted {len(records)} {label} nodes")

    async def upsert_edges(self, session, edges: List[Dict[str, Any]]) -> None:
        grouped_edges: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}

        for edge in edges:
            edge_type = edge.get("type")
            from_node = edge.get("from", {})
            to_node = edge.get("to", {})

            from_label = from_node.get("label")
            from_id = from_node.get("id")
            to_label = to_node.get("label")
            to_id = to_node.get("id")

            if not (
                is_safe_neo4j_name(edge_type)
                and is_safe_neo4j_name(from_label)
                and is_safe_neo4j_name(to_label)
                and from_id
                and to_id
            ):
                continue

            grouped_edges.setdefault((from_label, edge_type, to_label), []).append(
                {
                    "from_id": str(from_id),
                    "to_id": str(to_id),
                    "properties": edge.get("properties", {}),
                }
            )

        for (from_label, edge_type, to_label), records in grouped_edges.items():
            query = f"""
            UNWIND $records AS record
            MATCH (source:{from_label} {{id: record.from_id}})
            MATCH (target:{to_label} {{id: record.to_id}})
            MERGE (source)-[r:{edge_type}]->(target)
            SET r += record.properties
            """
            await session.run(query, records=records)
            print(f"Upserted {len(records)} {edge_type} relationships")

    async def print_summary(self, session) -> None:
        result = await session.run(
            """
            MATCH (n)
            RETURN labels(n)[0] AS node_type, count(*) AS count
            ORDER BY count DESC, node_type ASC
            """
        )
        rows = await result.data()
        print("Node counts after import:")
        for row in rows:
            print(f"  {row['node_type']}: {row['count']}")


async def main() -> None:
    ingestor = AnalysisReportIngestor(REPORT_PATH)
    await ingestor.ingest()


if __name__ == "__main__":
    asyncio.run(main())
