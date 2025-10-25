/**
 * This file demonstrates TypeScript autocomplete with multiple labels.
 * Open this in your IDE and place your cursor after the opening brace
 * in each addNode call to see autocomplete suggestions.
 */

import { SombraDB } from "../typed";

interface DemoSchema {
  nodes: {
    Person: {
      name: string;
      age: number;
      email?: string;
    };
    Employee: {
      employeeId: string;
      department?: string;
      salary?: number;
    };
    Manager: {
      reportsTo: number;
      level: number;
    };
  };
  edges: {};
}

const db = new SombraDB<DemoSchema>("./demo.db");

// Single label - autocomplete shows: name, age, email (all required)
const person = db.addNode("Person", {
  name: "Alice",
  age: 30,
  email: "alice@example.com",
});

// Multiple labels - autocomplete shows all properties from both labels
// Try typing here - you should see: name, age, email, employeeId, department, salary
const employee = db.addNode(["Person", "Employee"], {
  // Autocomplete available here! ⬇️
  name: "Bob",
  age: 25,
  employeeId: "E123",
  department: "Finance",
  salary: 95000,
  // All required properties from Person and Employee must be provided
});

// Three labels - autocomplete shows properties from all three
// Try typing here - you should see all properties from Person, Employee, and Manager
const manager = db.addNode(["Person", "Employee", "Manager"], {
  // Autocomplete available here! ⬇️
  name: "Charlie",
  age: 35,
  employeeId: "E456",
  department: "Engineering",
  level: 2,
  reportsTo: person,
});

// This would error if uncommented:
// const invalid = db.addNode<"Person", "InvalidLabel">(["Person", "InvalidLabel"], {});
//                                      ^^^^^^^^^^^^^^ Error: Type '"InvalidLabel"' does not satisfy constraint

console.log("Autocomplete demo - check your IDE for suggestions!");
