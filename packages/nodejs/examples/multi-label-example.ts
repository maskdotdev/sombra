import { SombraDB } from "../typed";
import * as fs from "fs";

// Define schema with multiple potential label combinations
interface OrgGraphSchema {
  nodes: {
    Person: {
      name: string;
      age: number;
    };
    Employee: {
      employeeId: string;
      department: string;
      salary: number;
    };
    Manager: {
      reportsTo: number;
      level: number;
    };
  };
  edges: {
    MANAGES: {
      from: "Manager";
      to: "Employee";
      properties: {};
    };
  };
}

const dbPath = "./multi-label-example.db";

// Clean up if exists
if (fs.existsSync(dbPath)) {
  fs.unlinkSync(dbPath);
}

console.log("Multi-Label Node Example\n");

const db = new SombraDB<OrgGraphSchema>(dbPath);

// CEO - just a Person
console.log("1. Creating CEO (Person only)...");
const ceo = db.addNode(["Person"], {
  name: "Sarah Chen",
  age: 45,
});
console.log(`Created CEO: ${ceo}\n`);

// VP - Person, Employee, and Manager
console.log("2. Creating VP (Person + Employee + Manager)...");
const vp = db.addNode(["Person", "Employee", "Manager"], {
  name: "John Smith",
  age: 38,
  employeeId: "E001",
  department: "Engineering",
  salary: 150000,
  reportsTo: ceo,
  level: 1,
});
console.log(`Created VP: ${vp}\n`);

// Regular employee - Person and Employee
console.log("3. Creating Engineer (Person + Employee)...");
const engineer = db.addNode(["Person", "Employee"], {
  name: "Alice Johnson",
  age: 28,
  employeeId: "E123",
  department: "Engineering",
  salary: 120000,
});
console.log(`Created Engineer: ${engineer}\n`);

// Create management relationship
db.addEdge(vp, engineer, "MANAGES", {});

// Query by different labels
console.log("4. Querying by labels...");
const allPeople = db.getNodesByLabel("Person");
const allEmployees = db.getNodesByLabel("Employee");
const allManagers = db.getNodesByLabel("Manager");

console.log(`People: ${allPeople.length} (nodes: ${allPeople.join(", ")})`);
console.log(
  `Employees: ${allEmployees.length} (nodes: ${allEmployees.join(", ")})`,
);
console.log(
  `Managers: ${allManagers.length} (nodes: ${allManagers.join(", ")})\n`,
);

// Retrieve and inspect nodes
console.log("5. Inspecting nodes...");
const vpNode = db.getNode(vp);
console.log("VP Node:", {
  id: vpNode?.id,
  labels: vpNode?.labels,
  properties: vpNode?.properties,
});

const engineerNode = db.getNode(engineer);
console.log("\nEngineer Node:", {
  id: engineerNode?.id,
  labels: engineerNode?.labels,
  properties: engineerNode?.properties,
});

console.log("\n✅ Multi-label example complete!");

// Cleanup
fs.unlinkSync(dbPath);
