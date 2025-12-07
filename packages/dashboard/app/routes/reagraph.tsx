/**
 * @deprecated This route is deprecated. The main /graph route now uses Reagraph by default.
 * This file redirects to /graph for backwards compatibility.
 */
import { useEffect } from "react";
import { useNavigate } from "react-router";

export function meta() {
    return [
        { title: "Redirecting to Graph Explorer · Sombra" },
    ];
}

export default function ReagraphExplorer() {
    const navigate = useNavigate();
    
    useEffect(() => {
        navigate("/graph", { replace: true });
    }, [navigate]);

    return (
        <div className="flex items-center justify-center h-screen text-muted-foreground">
            <p>Redirecting to Graph Explorer...</p>
        </div>
    );
}
