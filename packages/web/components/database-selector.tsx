'use client';

import { useState, useEffect } from 'react';

declare global {
  interface Window {
    validationTimeout?: NodeJS.Timeout;
  }
}

interface DatabaseSelectorProps {
  onDatabaseChange?: (path: string) => void;
}

export function DatabaseSelector({ onDatabaseChange }: DatabaseSelectorProps) {
  const [dbPath, setDbPath] = useState('');
  const [isValidating, setIsValidating] = useState(false);
  const [isValid, setIsValid] = useState<boolean | null>(null);
  const [stats, setStats] = useState<{ nodeCount: number; edgeCount: number } | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Load saved path from localStorage
    const savedPath = localStorage.getItem('sombra-db-path');
    if (savedPath) {
      setDbPath(savedPath);
      validateDatabase(savedPath);
    }
  }, []);

  const validateDatabase = async (path: string) => {
    if (!path.trim()) {
      setIsValid(null);
      setStats(null);
      setError(null);
      return;
    }

    setIsValidating(true);
    setError(null);

    try {
      const response = await fetch('/api/graph/stats', {
        headers: {
          'X-Database-Path': path,
        },
      });

      if (response.ok) {
        const data = await response.json();
        setIsValid(true);
        setStats(data);
        localStorage.setItem('sombra-db-path', path);
        onDatabaseChange?.(path);
      } else {
        const errorData = await response.json();
        setIsValid(false);
        setStats(null);
        setError(errorData.error || 'Failed to connect to database');
      }
    } catch (err) {
      setIsValid(false);
      setStats(null);
      setError(err instanceof Error ? err.message : 'Connection failed');
    } finally {
      setIsValidating(false);
    }
  };

  const handlePathChange = (newPath: string) => {
    setDbPath(newPath);
    // Debounce validation
    const timeoutId = setTimeout(() => {
      validateDatabase(newPath);
    }, 500);
    return () => clearTimeout(timeoutId);
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newPath = e.target.value;
    setDbPath(newPath);
    // Clear previous timeout
    if (window.validationTimeout) {
      clearTimeout(window.validationTimeout);
    }
    // Set new timeout for validation
    window.validationTimeout = setTimeout(() => {
      validateDatabase(newPath);
    }, 500);
  };

  const getStatusIcon = () => {
    if (isValidating) {
      return <div className="w-3 h-3 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />;
    }
    if (isValid === true) {
      return <div className="w-3 h-3 bg-green-500 rounded-full" />;
    }
    if (isValid === false) {
      return <div className="w-3 h-3 bg-red-500 rounded-full" />;
    }
    return <div className="w-3 h-3 bg-gray-400 rounded-full" />;
  };

  return (
    <div className="bg-gradient-to-r from-gray-800 to-gray-900 border-b border-gray-700 p-4 shadow-md">
      <div className="max-w-7xl mx-auto">
        <div className="flex items-center gap-4">
          <div className="flex-1">
            <label htmlFor="db-path" className="block text-sm font-medium text-gray-300 mb-2">
              Database Connection
            </label>
            <div className="flex items-center gap-3">
              <input
                id="db-path"
                type="text"
                value={dbPath}
                onChange={handleInputChange}
                placeholder="Enter database path (e.g., ./data.db, :memory:)"
                className="flex-1 px-4 py-2.5 bg-gray-700 border border-gray-600 text-white placeholder-gray-400 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
              />
              {getStatusIcon()}
              <button
                onClick={() => {
                  setDbPath('./demo.db');
                  validateDatabase('./demo.db');
                }}
                className="px-4 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors whitespace-nowrap"
              >
                Load Demo
              </button>
            </div>
            {error && (
              <p className="text-red-400 text-sm mt-2 flex items-center gap-2">
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                </svg>
                {error}
              </p>
            )}
            {isValid && (
              <p className="text-green-400 text-sm mt-2 flex items-center gap-2">
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                </svg>
                Connected successfully
              </p>
            )}
          </div>
          
          {stats && (
            <div className="text-sm text-gray-300 bg-gray-700 px-4 py-3 rounded-lg">
              <div className="flex gap-4">
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 bg-blue-400 rounded-full"></div>
                  <span>{stats.nodeCount} nodes</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 bg-green-400 rounded-full"></div>
                  <span>{stats.edgeCount} edges</span>
                </div>
              </div>
            </div>
          )}
        </div>
        
        <div className="mt-3 text-xs text-gray-400">
          <p>
            Examples: 
            <code className="ml-2 bg-gray-700 px-2 py-1 rounded text-gray-300">./data.db</code>
            <code className="ml-2 bg-gray-700 px-2 py-1 rounded text-gray-300">:memory:</code>
            <code className="ml-2 bg-gray-700 px-2 py-1 rounded text-gray-300">/absolute/path/database.db</code>
          </p>
        </div>
      </div>
    </div>
  );
}
