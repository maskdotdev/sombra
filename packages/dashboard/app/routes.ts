import { type RouteConfig, index, route } from "@react-router/dev/routes";

export default [
  index("routes/home.tsx"),
  route("graph", "routes/graph.tsx"),
  route("reagraph", "routes/reagraph.tsx"),
] satisfies RouteConfig;
