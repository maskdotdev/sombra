import { SombraDB, SombraPropertyValue } from 'sombradb';

const createProp = (type: 'string' | 'int' | 'float' | 'bool', value: any): SombraPropertyValue => ({
  type,
  value
});

export function seedCodeGraph(db: SombraDB) {
  console.log('🌱 Seeding code analysis graph...\n');

  const userServiceFile = db.addNode(['File'], {
    name: createProp('string', 'user-service.ts'),
    path: createProp('string', 'src/services/user-service.ts'),
    lines: createProp('int', 250)
  });

  const authServiceFile = db.addNode(['File'], {
    name: createProp('string', 'auth-service.ts'),
    path: createProp('string', 'src/services/auth-service.ts'),
    lines: createProp('int', 180)
  });

  const userControllerFile = db.addNode(['File'], {
    name: createProp('string', 'user-controller.ts'),
    path: createProp('string', 'src/controllers/user-controller.ts'),
    lines: createProp('int', 320)
  });

  const databaseFile = db.addNode(['File'], {
    name: createProp('string', 'database.ts'),
    path: createProp('string', 'src/db/database.ts'),
    lines: createProp('int', 150)
  });

  const loggerFile = db.addNode(['File'], {
    name: createProp('string', 'logger.ts'),
    path: createProp('string', 'src/utils/logger.ts'),
    lines: createProp('int', 80)
  });

  const userServiceClass = db.addNode(['Class'], {
    name: createProp('string', 'UserService'),
    exported: createProp('bool', true)
  });
  db.addEdge(userServiceFile, userServiceClass, 'CONTAINS');

  const authServiceClass = db.addNode(['Class'], {
    name: createProp('string', 'AuthService'),
    exported: createProp('bool', true)
  });
  db.addEdge(authServiceFile, authServiceClass, 'CONTAINS');

  const userControllerClass = db.addNode(['Class'], {
    name: createProp('string', 'UserController'),
    exported: createProp('bool', true)
  });
  db.addEdge(userControllerFile, userControllerClass, 'CONTAINS');

  const databaseClass = db.addNode(['Class'], {
    name: createProp('string', 'Database'),
    exported: createProp('bool', true)
  });
  db.addEdge(databaseFile, databaseClass, 'CONTAINS');

  const createUser = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'createUser'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(userServiceClass, createUser, 'CONTAINS');

  const updateUser = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'updateUser'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(userServiceClass, updateUser, 'CONTAINS');

  const deleteUser = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'deleteUser'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(userServiceClass, deleteUser, 'CONTAINS');

  const getUserById = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'getUserById'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(userServiceClass, getUserById, 'CONTAINS');

  const validateUser = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'validateUser'),
    async: createProp('bool', false),
    public: createProp('bool', false)
  });
  db.addEdge(userServiceClass, validateUser, 'CONTAINS');

  const hashPassword = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'hashPassword'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(authServiceClass, hashPassword, 'CONTAINS');

  const verifyPassword = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'verifyPassword'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(authServiceClass, verifyPassword, 'CONTAINS');

  const generateToken = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'generateToken'),
    async: createProp('bool', false),
    public: createProp('bool', true)
  });
  db.addEdge(authServiceClass, generateToken, 'CONTAINS');

  const handleCreateUser = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'handleCreateUser'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(userControllerClass, handleCreateUser, 'CONTAINS');

  const handleUpdateUser = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'handleUpdateUser'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(userControllerClass, handleUpdateUser, 'CONTAINS');

  const handleDeleteUser = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'handleDeleteUser'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(userControllerClass, handleDeleteUser, 'CONTAINS');

  const query = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'query'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(databaseClass, query, 'CONTAINS');

  const connect = db.addNode(['Function', 'Method'], {
    name: createProp('string', 'connect'),
    async: createProp('bool', true),
    public: createProp('bool', true)
  });
  db.addEdge(databaseClass, connect, 'CONTAINS');

  const logInfo = db.addNode(['Function'], {
    name: createProp('string', 'logInfo'),
    async: createProp('bool', false),
    exported: createProp('bool', true)
  });
  db.addEdge(loggerFile, logInfo, 'CONTAINS');

  const logError = db.addNode(['Function'], {
    name: createProp('string', 'logError'),
    async: createProp('bool', false),
    exported: createProp('bool', true)
  });
  db.addEdge(loggerFile, logError, 'CONTAINS');

  db.addEdge(handleCreateUser, createUser, 'CALLS', {
    lineNumber: createProp('int', 45)
  });
  db.addEdge(handleCreateUser, validateUser, 'CALLS', {
    lineNumber: createProp('int', 42)
  });
  db.addEdge(handleCreateUser, hashPassword, 'CALLS', {
    lineNumber: createProp('int', 48)
  });
  db.addEdge(handleCreateUser, logInfo, 'CALLS', {
    lineNumber: createProp('int', 52)
  });

  db.addEdge(handleUpdateUser, updateUser, 'CALLS', {
    lineNumber: createProp('int', 65)
  });
  db.addEdge(handleUpdateUser, getUserById, 'CALLS', {
    lineNumber: createProp('int', 62)
  });
  db.addEdge(handleUpdateUser, validateUser, 'CALLS', {
    lineNumber: createProp('int', 63)
  });
  db.addEdge(handleUpdateUser, logInfo, 'CALLS', {
    lineNumber: createProp('int', 68)
  });

  db.addEdge(handleDeleteUser, deleteUser, 'CALLS', {
    lineNumber: createProp('int', 82)
  });
  db.addEdge(handleDeleteUser, getUserById, 'CALLS', {
    lineNumber: createProp('int', 80)
  });
  db.addEdge(handleDeleteUser, logInfo, 'CALLS', {
    lineNumber: createProp('int', 84)
  });

  db.addEdge(createUser, query, 'CALLS', {
    lineNumber: createProp('int', 125)
  });
  db.addEdge(createUser, logError, 'CALLS', {
    lineNumber: createProp('int', 128)
  });

  db.addEdge(updateUser, query, 'CALLS', {
    lineNumber: createProp('int', 145)
  });
  db.addEdge(updateUser, getUserById, 'CALLS', {
    lineNumber: createProp('int', 142)
  });

  db.addEdge(deleteUser, query, 'CALLS', {
    lineNumber: createProp('int', 165)
  });

  db.addEdge(getUserById, query, 'CALLS', {
    lineNumber: createProp('int', 185)
  });

  db.addEdge(hashPassword, logError, 'CALLS', {
    lineNumber: createProp('int', 95)
  });

  db.addEdge(verifyPassword, logError, 'CALLS', {
    lineNumber: createProp('int', 112)
  });

  db.addEdge(userControllerFile, userServiceFile, 'IMPORTS');
  db.addEdge(userControllerFile, authServiceFile, 'IMPORTS');
  db.addEdge(userControllerFile, loggerFile, 'IMPORTS');
  db.addEdge(userServiceFile, databaseFile, 'IMPORTS');
  db.addEdge(userServiceFile, loggerFile, 'IMPORTS');
  db.addEdge(authServiceFile, loggerFile, 'IMPORTS');

  console.log('✅ Seeded graph with:');
  console.log('   - 5 files');
  console.log('   - 4 classes');
  console.log('   - 15 functions/methods');
  console.log('   - Multiple CALLS, CONTAINS, and IMPORTS relationships\n');

  return {
    files: {
      userServiceFile,
      authServiceFile,
      userControllerFile,
      databaseFile,
      loggerFile
    },
    classes: {
      userServiceClass,
      authServiceClass,
      userControllerClass,
      databaseClass
    },
    functions: {
      createUser,
      updateUser,
      deleteUser,
      getUserById,
      validateUser,
      hashPassword,
      verifyPassword,
      generateToken,
      handleCreateUser,
      handleUpdateUser,
      handleDeleteUser,
      query,
      connect,
      logInfo,
      logError
    }
  };
}
