const express = require('express');
const cors = require('cors');
const sqlite3 = require('sqlite3').verbose();
const WebSocket = require('ws');
const http = require('http');
const { initializeApp } = require('firebase/app');
const { getDatabase, ref, set, push, onValue, get } = require('firebase/database');

const app = express();
app.use(cors());
app.use(express.json());

const db = new sqlite3.Database('./pipeline.db');

// ==================== FIREBASE SETUP ====================

const firebaseConfig = {
  databaseURL: "https://jal-mahakal-shakti-default-rtdb.asia-southeast1.firebasedatabase.app"
};

const firebaseApp = initializeApp(firebaseConfig);
const firebaseDb = getDatabase(firebaseApp);

// ==================== CONSTANTS ====================

const WATER_DENSITY = 1000; // kg/m³
const GRAVITY = 9.81; // m/s²

// ==================== DATABASE SETUP ====================

db.serialize(() => {





    // First, check and add missing columns if they don't exist
    db.all("PRAGMA table_info(pipelines)", [], (err, columns) => {
        if (err) {
            console.error('Error checking pipelines schema:', err);
            return;
        }
        
        const hasActiveColumn = columns && columns.some(col => col.name === 'active');
        
        if (!hasActiveColumn) {
            console.log('⚙️ Adding missing "active" column to pipelines table...');
            db.run(`ALTER TABLE pipelines ADD COLUMN active INTEGER DEFAULT 1`, (err) => {
                if (err && !err.message.includes('duplicate column')) {
                    console.error('Error adding active column:', err);
                } else {
                    console.log('✅ Added "active" column to pipelines table');
                }
            });
        }
    });
    // Tanks table (with all required fields from HTML)
    db.run(`
        CREATE TABLE IF NOT EXISTS tanks (
            tankId TEXT PRIMARY KEY,
            deviceId TEXT,
            name TEXT NOT NULL,
            state TEXT,
            district TEXT,
            mandal TEXT,
            habitation TEXT,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            type TEXT NOT NULL,
            shape TEXT NOT NULL DEFAULT 'cylinder',
            diameter REAL NOT NULL DEFAULT 5.0,
            height REAL NOT NULL DEFAULT 10.0,
            sensorHeight REAL NOT NULL DEFAULT 10.0,
            capacity REAL NOT NULL,
            waterLevel REAL DEFAULT 8.5,
            isActive INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);
    
    // Valves table (with flowRate field)
    db.run(`
        CREATE TABLE IF NOT EXISTS gate_valves (
            valveId TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'STRAIGHT',
            category TEXT NOT NULL DEFAULT 'main',
            parentValveId TEXT,
            households INTEGER NOT NULL DEFAULT 0,
            flowRate REAL DEFAULT 0,
            mandal TEXT,
            habitation TEXT,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            isOpen INTEGER DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (parentValveId) REFERENCES gate_valves(valveId) ON DELETE SET NULL
        )
    `);
    
    // Pipelines table (with nodes as JSON)
    // Pipelines table (with nodes as JSON)
    db.run(`
        CREATE TABLE IF NOT EXISTS pipelines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'PVC',
            diameter REAL NOT NULL DEFAULT 150,
            capacity REAL NOT NULL DEFAULT 500,
            startPoint TEXT,
            endPoint TEXT,
            notes TEXT,
            nodes TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `, (err) => {
        if (err) {
            console.error('❌ Error creating pipelines table:', err);
        } else {
            console.log('✅ Pipelines table ready');
        }
    });
    
    // Tank history table
    db.run(`
        CREATE TABLE IF NOT EXISTS tank_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deviceId TEXT NOT NULL,
            tankId TEXT NOT NULL,
            name TEXT NOT NULL,
            distance REAL NOT NULL,
            waterLevel REAL NOT NULL,
            waterLevelCm REAL NOT NULL,
            volume REAL NOT NULL,
            percentage REAL NOT NULL,
            pressure REAL NOT NULL,
            weight REAL NOT NULL,
            status TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);
    
    // Valve history table
    db.run(`
        CREATE TABLE IF NOT EXISTS valve_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            valveId TEXT NOT NULL,
            name TEXT NOT NULL,
            isOpen INTEGER NOT NULL,
            flowRate REAL NOT NULL,
            pressure REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);
    
    // Sensor data cache table
    db.run(`
        CREATE TABLE IF NOT EXISTS sensor_cache (
            deviceId TEXT PRIMARY KEY,
            tankId TEXT NOT NULL,
            name TEXT NOT NULL,
            waterLevel REAL NOT NULL,
            volume REAL NOT NULL,
            percentage REAL NOT NULL,
            pressure REAL NOT NULL,
            weight REAL NOT NULL,
            status TEXT NOT NULL,
            lastUpdated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `, (err) => {
        if (err) {
            console.error('❌ Error creating tables:', err);
        } else {
            console.log('✅ All tables created successfully');
            initializeFirebaseSync();
        }
    });
});

// ==================== TANK CALCULATION FUNCTIONS ====================

function calculateCylindricalTank(radius, tankHeight, liquidHeight) {
    const radiusM = radius;
    const volumeM3 = Math.PI * Math.pow(radiusM, 2) * liquidHeight;
    const volumeLiters = volumeM3 * 1000;
    const weightKg = volumeM3 * WATER_DENSITY;
    const pressurePa = WATER_DENSITY * GRAVITY * liquidHeight;
    const pressureKPa = pressurePa / 1000;
    const fillPercentage = (liquidHeight / tankHeight) * 100;
    
    return {
        volume: volumeLiters,
        weight: weightKg,
        pressure: pressureKPa,
        percentage: fillPercentage
    };
}

function calculateCuboidTank(length, width, tankHeight, liquidHeight) {
    const volumeM3 = length * width * liquidHeight;
    const volumeLiters = volumeM3 * 1000;
    const weightKg = volumeM3 * WATER_DENSITY;
    const pressurePa = WATER_DENSITY * GRAVITY * liquidHeight;
    const pressureKPa = pressurePa / 1000;
    const fillPercentage = (liquidHeight / tankHeight) * 100;
    
    return {
        volume: volumeLiters,
        weight: weightKg,
        pressure: pressureKPa,
        percentage: fillPercentage
    };
}


// ==================== CAPACITY CALCULATION ====================

function calculateTankCapacity(diameter, height, shape) {
    shape = shape.toLowerCase();
    
    if (shape === 'cylinder' || shape === 'cylindrical') {
        const radius = diameter / 2;
        const volumeM3 = Math.PI * Math.pow(radius, 2) * height;
        return parseFloat((volumeM3 * 1000).toFixed(2)); // Convert to liters
    } else if (shape === 'cuboid' || shape === 'rectangular' || shape === 'square') {
        const volumeM3 = diameter * diameter * height; // Assuming square base
        return parseFloat((volumeM3 * 1000).toFixed(2));
    }
    
    return 0;
}

function calculateTankData(deviceId, distanceCm, tankParams) {
    if (!tankParams) {
        console.warn(`⚠️ No parameters found for ${deviceId}`);
        return null;
    }

    const sensorHeightM = tankParams.sensorHeight;
    const tankHeightM = tankParams.height;
    const diameterM = tankParams.diameter;
    const capacityL = tankParams.capacity;
    const shape = tankParams.shape.toLowerCase();
    
    const distanceM = distanceCm / 100;
    const liquidHeightM = sensorHeightM - distanceM;
    const actualLiquidHeightM = Math.max(0, Math.min(liquidHeightM, tankHeightM));
    
    let calculations;
    
    if (shape === 'cylinder' || shape === 'cylindrical') {
        const radiusM = diameterM / 2;
        calculations = calculateCylindricalTank(radiusM, tankHeightM, actualLiquidHeightM);
    } else if (shape === 'cuboid' || shape === 'rectangular' || shape === 'square') {
        const lengthM = diameterM;
        const widthM = (capacityL / 1000) / (lengthM * tankHeightM);
        calculations = calculateCuboidTank(lengthM, widthM, tankHeightM, actualLiquidHeightM);
    } else {
        console.error(`❌ Unknown tank shape: ${shape}`);
        return null;
    }
    
    let status = 'normal';
    if (calculations.percentage < 10) {
        status = 'low';
    } else if (calculations.percentage >= 80) {
        status = 'high';
    }

    return {
        deviceId: deviceId,
        tankId: tankParams.tankId || deviceId,
        name: tankParams.name || 'Unknown Tank',
        distance: distanceCm,
        waterLevel: parseFloat(actualLiquidHeightM.toFixed(2)),
        waterLevelCm: parseFloat((actualLiquidHeightM * 100).toFixed(2)),
        volume: parseFloat(calculations.volume.toFixed(2)),
        percentage: parseFloat(calculations.percentage.toFixed(2)),
        pressure: parseFloat(calculations.pressure.toFixed(2)),
        weight: parseFloat(calculations.weight.toFixed(2)),
        status: status,
        capacity: capacityL,
        shape: shape,
        height: tankHeightM,
        diameter: diameterM,
        sensorHeight: sensorHeightM,
        timestamp: Date.now(),
        lastUpdated: new Date().toISOString()
    };
}

// ==================== FLOW CALCULATION FUNCTIONS ====================

function calculateFlowPaths(tanks, valves, pipelines) {
    console.log('🔍 Calculating flow paths...');
    
    const CONNECT_DISTANCE = 50; // meters
    const VALVE_BLOCK_DISTANCE = 3; // meters
    const activeTanks = tanks.filter(t => t.isActive);
    
    if (activeTanks.length === 0) {
        console.log('⚠️ No active tanks for flow calculation');
        return { segments: [], blockedSegments: [], totalSegments: 0 };
    }
    
    const flowData = {
        segments: [],
        blockedSegments: [],
        totalSegments: 0
    };
    
    // Build unified graph from all pipelines
    const graph = buildPipelineGraph(pipelines, CONNECT_DISTANCE);
    
    // Get closed valves
    const closedValves = valves.filter(v => !v.isOpen);
    
    // BFS starting from each active tank
    const visitedEdges = new Set();
    const blockedEdges = new Set();
    const visitedNodes = new Set();
    
    activeTanks.forEach(tank => {
        const tankPos = { lat: tank.latitude, lng: tank.longitude };
        const queue = [];
        
        // Find starting nodes near tank
        graph.nodes.forEach((nodeData, nodeKey) => {
            const dist = haversineDistance(tankPos, nodeData.position);
            if (dist < CONNECT_DISTANCE) {
                queue.push({
                    nodeKey: nodeKey,
                    sourceTank: tank.name
                });
            }
        });
        
        // BFS traversal
        while (queue.length > 0) {
            const current = queue.shift();
            
            if (visitedNodes.has(current.nodeKey)) continue;
            visitedNodes.add(current.nodeKey);
            
            const currentNode = graph.nodes.get(current.nodeKey);
            if (!currentNode) continue;
            
            // Explore all connected edges
            currentNode.edges.forEach(edge => {
                const edgeKey = `${edge.from}-${edge.to}`;
                const reverseKey = `${edge.to}-${edge.from}`;
                
                if (visitedEdges.has(edgeKey) || visitedEdges.has(reverseKey)) return;
                if (blockedEdges.has(edgeKey) || blockedEdges.has(reverseKey)) return;
                
                // Check for blocking valve
                const startPos = graph.nodes.get(edge.from).position;
                const endPos = graph.nodes.get(edge.to).position;
                
                const blockingValve = findBlockingValve(startPos, endPos, closedValves, VALVE_BLOCK_DISTANCE);
                
                if (blockingValve) {
                    blockedEdges.add(edgeKey);
                    blockedEdges.add(reverseKey);
                    flowData.blockedSegments.push({
                        pipelineId: edge.pipelineId,
                        start: startPos,
                        end: endPos,
                        blockedBy: blockingValve.name
                    });
                } else {
                    visitedEdges.add(edgeKey);
                    visitedEdges.add(reverseKey);
                    flowData.segments.push({
                        pipelineId: edge.pipelineId,
                        start: startPos,
                        end: endPos,
                        sourceTank: current.sourceTank,
                        hasFlow: true,
                        blocked: false
                    });
                    
                    // Add neighbor to queue
                    queue.push({
                        nodeKey: edge.to,
                        sourceTank: current.sourceTank
                    });
                }
            });
        }
    });
    
    // Calculate total segments
    pipelines.forEach(pipeline => {
        const nodes = JSON.parse(pipeline.nodes);
        flowData.totalSegments += nodes.length - 1;
    });
    
    console.log(`📊 Flow calculation complete: ${flowData.segments.length} flowing, ${flowData.blockedSegments.length} blocked`);
    return flowData;
}
function buildPipelineGraph(pipelines, connectDistance) {
    const graph = {
        nodes: new Map(),
        edges: []
    };
    
    const getNodeKey = (lat, lng) => {
        return `${lat.toFixed(6)},${lng.toFixed(6)}`;
    };
    
    const findNearbyNode = (position) => {
        for (let [nodeKey, nodeData] of graph.nodes) {
            const dist = haversineDistance(position, nodeData.position);
            if (dist < connectDistance) {
                return nodeKey;
            }
        }
        return null;
    };
    
    pipelines.forEach(pipeline => {
        // Parse nodes if they're a string
        let nodes;
        try {
            nodes = typeof pipeline.nodes === 'string' ? JSON.parse(pipeline.nodes) : pipeline.nodes;
        } catch (error) {
            console.error(`Error parsing nodes for pipeline ${pipeline.id}:`, error);
            nodes = [];
        }
        
        if (!Array.isArray(nodes) || nodes.length === 0) {
            console.warn(`Pipeline ${pipeline.id} has no valid nodes`);
            return;
        }
        
        const pipelineId = pipeline.id;
        
        for (let i = 0; i < nodes.length; i++) {
            // Validate node structure
            if (!nodes[i] || typeof nodes[i].lat !== 'number' || typeof nodes[i].lng !== 'number') {
                console.warn(`Invalid node at index ${i} in pipeline ${pipeline.id}:`, nodes[i]);
                continue;
            }
            
            const nodePos = { lat: nodes[i].lat, lng: nodes[i].lng };
            let nodeKey = findNearbyNode(nodePos);
            
            if (!nodeKey) {
                nodeKey = getNodeKey(nodePos.lat, nodePos.lng);
                graph.nodes.set(nodeKey, {
                    position: nodePos,
                    edges: []
                });
            }
            
            // Create edge to next node
            if (i < nodes.length - 1) {
                const nextNode = nodes[i + 1];
                
                // Validate next node
                if (!nextNode || typeof nextNode.lat !== 'number' || typeof nextNode.lng !== 'number') {
                    console.warn(`Invalid next node at index ${i + 1} in pipeline ${pipeline.id}:`, nextNode);
                    continue;
                }
                
                const nextNodePos = { lat: nextNode.lat, lng: nextNode.lng };
                let nextNodeKey = findNearbyNode(nextNodePos);
                
                if (!nextNodeKey) {
                    nextNodeKey = getNodeKey(nextNodePos.lat, nextNodePos.lng);
                    graph.nodes.set(nextNodeKey, {
                        position: nextNodePos,
                        edges: []
                    });
                }
                
                // Add bidirectional edges
                const edge = {
                    from: nodeKey,
                    to: nextNodeKey,
                    pipelineId: pipelineId
                };
                
                graph.nodes.get(nodeKey).edges.push(edge);
                
                const reverseEdge = {
                    from: nextNodeKey,
                    to: nodeKey,
                    pipelineId: pipelineId
                };
                
                graph.nodes.get(nextNodeKey).edges.push(reverseEdge);
                graph.edges.push(edge);
            }
        }
    });
    
    return graph;
}

function findBlockingValve(segmentStart, segmentEnd, closedValves, maxDistance) {
    for (let valve of closedValves) {
        const valvePos = { lat: valve.latitude, lng: valve.longitude };
        
        // Calculate the closest point on the segment to the valve
        const A = valvePos.lat - segmentStart.lat;
        const B = valvePos.lng - segmentStart.lng;
        const C = segmentEnd.lat - segmentStart.lat;
        const D = segmentEnd.lng - segmentStart.lng;
        
        const dot = A * C + B * D;
        const lenSq = C * C + D * D;
        
        if (lenSq === 0) continue; // Zero length segment
        
        let param = dot / lenSq;
        
        // Check if the closest point is within the segment (not beyond endpoints)
        if (param < 0 || param > 1) continue;
        
        // Calculate the closest point on the segment
        const closestLat = segmentStart.lat + param * C;
        const closestLng = segmentStart.lng + param * D;
        
        // Calculate distance from valve to closest point on segment
        const dist = haversineDistance(
            valvePos,
            { lat: closestLat, lng: closestLng }
        );
        
        // Only block if valve is very close to the segment (within maxDistance)
        if (dist < maxDistance) {
            console.log(`🚫 Valve "${valve.name}" blocking segment at distance ${dist.toFixed(2)}m`);
            return valve;
        }
    }
    return null;
}

function haversineDistance(pos1, pos2) {
    const R = 6371000; // Earth's radius in meters
    const lat1 = pos1.lat * Math.PI / 180;
    const lat2 = pos2.lat * Math.PI / 180;
    const deltaLat = (pos2.lat - pos1.lat) * Math.PI / 180;
    const deltaLng = (pos2.lng - pos1.lng) * Math.PI / 180;
    
    const a = Math.sin(deltaLat/2) * Math.sin(deltaLat/2) +
              Math.cos(lat1) * Math.cos(lat2) *
              Math.sin(deltaLng/2) * Math.sin(deltaLng/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    
    return R * c;
}

function distanceToSegment(point, segmentStart, segmentEnd) {
    const A = point.lat - segmentStart.lat;
    const B = point.lng - segmentStart.lng;
    const C = segmentEnd.lat - segmentStart.lat;
    const D = segmentEnd.lng - segmentStart.lng;
    
    const dot = A * C + B * D;
    const lenSq = C * C + D * D;
    let param = -1;
    
    if (lenSq !== 0) param = dot / lenSq;
    
    let xx, yy;
    
    if (param < 0) {
        xx = segmentStart.lat;
        yy = segmentStart.lng;
    } else if (param > 1) {
        xx = segmentEnd.lat;
        yy = segmentEnd.lng;
    } else {
        xx = segmentStart.lat + param * C;
        yy = segmentStart.lng + param * D;
    }
    
    return haversineDistance(point, {lat: xx, lng: yy});
}

// ==================== SUPPLY OVERVIEW FUNCTIONS ====================

function calculateSupplyOverview(tanks, valves, pipelines) {
    console.log('📈 Calculating supply overview...');
    
    const flowData = calculateFlowPaths(tanks, valves, pipelines);
    const mainValves = valves.filter(v => v.category === 'main');
    const subValves = valves.filter(v => v.category === 'sub');
    
    // Build valve tree
    const valveTree = new Map();
    mainValves.forEach(valve => {
        valveTree.set(valve.valveId, {
            valve: valve,
            children: subValves.filter(sv => sv.parentValveId === valve.valveId),
            totalHouseholds: valve.households,
            directHouseholds: valve.households, // Will be recalculated
            servedHouseholds: 0,
            totalFlow: 0
        });
    });
    
    // Calculate direct households for main valves
    mainValves.forEach(mainValve => {
        const node = valveTree.get(mainValve.valveId);
        if (!node) return;
        
        const subValvesTotal = node.children.reduce((sum, child) => sum + child.households, 0);
        node.directHouseholds = Math.max(0, node.totalHouseholds - subValvesTotal);
    });
    
    // Calculate flow for each valve
    valveTree.forEach(node => {
        if (node.valve.isOpen) {
            // Estimate flow based on connected pipelines
            const connectedPipelines = findConnectedPipelines(node.valve, pipelines);
            const totalFlow = connectedPipelines.reduce((sum, pipe) => {
                // Check if pipeline has flow
                const hasFlow = flowData.segments.some(s => s.pipelineId === pipe.id);
                return sum + (hasFlow ? pipe.capacity * 0.8 : 0); // 80% of capacity if flowing
            }, 0);
            
            node.totalFlow = totalFlow;
        } else {
            node.totalFlow = 0;
        }
    });
    
    // Calculate served households
    let totalHouseholds = 0;
    let servedHouseholds = 0;
    let totalFlow = 0;
    
    valveTree.forEach(node => {
        totalHouseholds += node.totalHouseholds;
        
        if (node.valve.isOpen) {
            // Distribute flow among children and direct households
            const openChildren = node.children.filter(c => c.isOpen);
            const childrenHouseholds = openChildren.reduce((sum, c) => sum + c.households, 0);
            const totalOpenHouseholds = node.directHouseholds + childrenHouseholds;
            
            if (totalOpenHouseholds > 0) {
                // Calculate served households
                const served = Math.min(totalOpenHouseholds, Math.floor(node.totalFlow / 10)); // Assume 10 L/min per household
                node.servedHouseholds = served;
                servedHouseholds += served;
                
                // Distribute flow
                const flowPerHousehold = node.totalFlow / totalOpenHouseholds;
                node.directFlow = node.directHouseholds * flowPerHousehold;
                openChildren.forEach(child => {
                    const childNode = valveTree.get(child.valveId);
                    if (childNode) {
                        childNode.totalFlow = child.households * flowPerHousehold;
                    }
                });
            }
        }
        
        totalFlow += node.totalFlow;
    });
    
    const coverage = totalHouseholds > 0 ? (servedHouseholds / totalHouseholds) * 100 : 0;
    const avgSupplyPerHousehold = servedHouseholds > 0 ? totalFlow / servedHouseholds : 0;
    
    // Build regions
    const regions = {};
    valves.forEach(valve => {
        if (!regions[valve.mandal]) {
            regions[valve.mandal] = {
                name: valve.mandal,
                valves: [],
                totalHouseholds: 0,
                servedHouseholds: 0,
                totalFlow: 0
            };
        }
        
        const node = valveTree.get(valve.valveId);
        if (node) {
            regions[valve.mandal].valves.push({
                ...valve,
                flow: node.totalFlow,
                households: valve.households,
                isOpen: valve.isOpen,
                servedHouseholds: node.servedHouseholds || 0
            });
            regions[valve.mandal].totalHouseholds += valve.households;
            regions[valve.mandal].servedHouseholds += node.servedHouseholds || 0;
            regions[valve.mandal].totalFlow += node.totalFlow;
        }
    });
    
    return {
        stats: {
            totalHouseholds,
            servedHouseholds,
            coverage: parseFloat(coverage.toFixed(1)),
            totalFlow: parseFloat(totalFlow.toFixed(0)),
            avgSupplyPerHousehold: parseFloat(avgSupplyPerHousehold.toFixed(1)),
            mainValves: mainValves.length,
            subValves: subValves.length,
            activeTanks: tanks.filter(t => t.isActive).length
        },
        regions: Object.values(regions),
        valveTree: Array.from(valveTree.values())
    };
}

function findConnectedPipelines(valve, pipelines) {
    const connected = [];
    const valvePos = { lat: valve.latitude, lng: valve.longitude };
    
    pipelines.forEach(pipe => {
        // Parse nodes if they're a string
        let nodes;
        try {
            nodes = typeof pipe.nodes === 'string' ? JSON.parse(pipe.nodes) : pipe.nodes;
        } catch (error) {
            console.error(`Error parsing nodes for pipeline ${pipe.id}:`, error);
            return;
        }
        
        if (!Array.isArray(nodes) || nodes.length === 0) {
            return;
        }
        
        for (let i = 0; i < nodes.length - 1; i++) {
            // Validate nodes
            if (!nodes[i] || !nodes[i + 1] || 
                typeof nodes[i].lat !== 'number' || typeof nodes[i].lng !== 'number' ||
                typeof nodes[i + 1].lat !== 'number' || typeof nodes[i + 1].lng !== 'number') {
                continue;
            }
            
            const dist = distanceToSegment(
                valvePos,
                { lat: nodes[i].lat, lng: nodes[i].lng },
                { lat: nodes[i + 1].lat, lng: nodes[i + 1].lng }
            );
            
            if (dist < 15) { // Within 15 meters
                connected.push(pipe);
                break;
            }
        }
    });
    
    return connected;
}

// ==================== FIREBASE SYNC FUNCTIONS ====================

async function syncTankParametersToFirebase() {
    console.log('🔄 Syncing tank parameters to Firebase...');
    
    return new Promise((resolve, reject) => {
        db.all('SELECT * FROM tanks WHERE isActive = 1', [], async (err, tanks) => {
            if (err) {
                console.error('❌ Error fetching tanks:', err);
                reject(err);
                return;
            }
            
            console.log(`📊 Found ${tanks.length} active tanks to sync`);
            
            for (const tank of tanks) {
                if (!tank.deviceId) {
                    console.warn(`⚠️ Tank ${tank.tankId} has no deviceId, skipping`);
                    continue;
                }
                
                try {
                    const paramRef = ref(firebaseDb, `tankParameters/${tank.deviceId}`);
                    
                    const parameters = {
                        tankId: tank.tankId,
                        deviceId: tank.deviceId,
                        name: tank.name,
                        state: tank.state || '',
                        district: tank.district || '',
                        mandal: tank.mandal || '',
                        habitation: tank.habitation || '',
                        latitude: tank.latitude,
                        longitude: tank.longitude,
                        type: tank.type,
                        shape: tank.shape,
                        diameter: tank.diameter,
                        height: tank.height,
                        sensorHeight: tank.sensorHeight,
                        capacity: tank.capacity,
                        waterDensity: WATER_DENSITY,
                        gravity: GRAVITY
                    };
                    
                    await set(paramRef, parameters);
                    console.log(`✅ Synced ${tank.deviceId} (${tank.name})`);
                } catch (error) {
                    console.error(`❌ Error syncing ${tank.deviceId}:`, error);
                }
            }
            
            resolve();
        });
    });
}

let TANK_PARAMETERS = {};

async function loadTankParameters() {
    console.log('🔥 Loading tank parameters from Firebase...');
    
    try {
        const paramsRef = ref(firebaseDb, 'tankParameters');
        const snapshot = await get(paramsRef);
        
        if (snapshot.exists()) {
            TANK_PARAMETERS = snapshot.val();
            console.log('✅ Tank parameters loaded:', Object.keys(TANK_PARAMETERS).length, 'devices');
            return true;
        } else {
            console.log('⚠️ No parameters found in Firebase');
            return true;
        }
    } catch (error) {
        console.error('❌ Error loading parameters:', error);
        return false;
    }
}

function watchParameterChanges() {
    const paramsRef = ref(firebaseDb, 'tankParameters');
    
    onValue(paramsRef, (snapshot) => {
        if (snapshot.exists()) {
            const newParams = snapshot.val();
            
            if (JSON.stringify(newParams) !== JSON.stringify(TANK_PARAMETERS)) {
                TANK_PARAMETERS = newParams;
                console.log('🔄 Tank parameters updated from Firebase!');
            }
        }
    });
}

// Store previous readings for flow rate calculation
const previousReadings = new Map();

function startListeningToDevice(deviceId) {
    const deviceRef = ref(firebaseDb, `devices/${deviceId}`);
    
    console.log(`👂 Listening to device: ${deviceId}`);
    
    onValue(deviceRef, (snapshot) => {
        const data = snapshot.val();
        
        if (data && data.distance !== undefined) {
            const tankParams = TANK_PARAMETERS[deviceId];
            
            if (!tankParams) {
                console.warn(`⚠️ No parameters for ${deviceId}, skipping calculation`);
                return;
            }
            
            console.log(`📡 ${deviceId} - Raw Distance: ${data.distance} cm`);
            const calculatedData = calculateTankData(deviceId, data.distance, tankParams);
            
            if (calculatedData) {
                // Calculate flow rate based on previous reading
                const prevReading = previousReadings.get(deviceId);
                let flowRate = 0;
                let flowStatus = 'stable';
                
                if (prevReading) {
                    const timeDiff = (calculatedData.timestamp - prevReading.timestamp) / 1000; // seconds
                    const levelDiff = calculatedData.waterLevel - prevReading.waterLevel; // meters
                    
                    if (timeDiff > 0) {
                        // Calculate flow rate in liters per minute
                        const volumeDiff = calculatedData.volume - prevReading.volume;
                        flowRate = (volumeDiff / timeDiff) * 60; // L/min
                        
                        // Determine flow status
                        if (Math.abs(flowRate) < 5) {
                            flowStatus = 'stable';
                        } else if (flowRate > 0) {
                            flowStatus = 'inflow';
                        } else {
                            flowStatus = 'outflow';
                        }
                        
                        console.log(`💧 ${deviceId} - Flow Rate: ${flowRate.toFixed(2)} L/min (${flowStatus})`);
                    }
                }
                
                // Store current reading for next comparison
                previousReadings.set(deviceId, {
                    waterLevel: calculatedData.waterLevel,
                    volume: calculatedData.volume,
                    timestamp: calculatedData.timestamp
                });
                
                // Enhanced status calculation
                let enhancedStatus = calculatedData.status;
                if (calculatedData.percentage < 10) {
                    enhancedStatus = 'critical';
                } else if (calculatedData.percentage < 20) {
                    enhancedStatus = 'low';
                } else if (flowRate < -50) {
                    enhancedStatus = 'draining';
                } else if (flowRate > 50) {
                    enhancedStatus = 'filling';
                } else if (calculatedData.percentage >= 80) {
                    enhancedStatus = 'high';
                } else {
                    enhancedStatus = 'normal';
                }
                
                // Add flow rate and enhanced status to calculated data
                const enrichedData = {
                    ...calculatedData,
                    flowRate: parseFloat(flowRate.toFixed(2)),
                    flowStatus: flowStatus,
                    status: enhancedStatus
                };
                
                // Store in Firebase with flow rate
                set(ref(firebaseDb, `tanks/${deviceId}`), enrichedData)
                    .then(() => {
                        console.log(`✅ ${deviceId} updated successfully with flow rate`);
                    })
                    .catch((error) => {
                        console.error(`❌ Error storing data for ${deviceId}:`, error.message);
                    });
                
                // Store in SQLite cache with flow rate
                db.run(
                    `INSERT OR REPLACE INTO sensor_cache (deviceId, tankId, name, waterLevel, volume, percentage, pressure, weight, status, lastUpdated)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
                    [enrichedData.deviceId, enrichedData.tankId, enrichedData.name, enrichedData.waterLevel,
                     enrichedData.volume, enrichedData.percentage, enrichedData.pressure, enrichedData.weight,
                     enrichedData.status],
                    function(err) {
                        if (err) {
                            console.error(`❌ Error storing cache for ${deviceId}:`, err.message);
                        } else {
                            console.log(`💾 ${deviceId} cached successfully`);
                        }
                    }
                );
                
                // Store in history with flow rate (AUTOMATIC HISTORY)
                db.run(
                    `INSERT INTO tank_history (deviceId, tankId, name, distance, waterLevel, waterLevelCm, volume, percentage, pressure, weight, status, timestamp)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [enrichedData.deviceId, enrichedData.tankId, enrichedData.name, enrichedData.distance,
                     enrichedData.waterLevel, enrichedData.waterLevelCm, enrichedData.volume, enrichedData.percentage,
                     enrichedData.pressure, enrichedData.weight, enrichedData.status, enrichedData.timestamp],
                    function(err) {
                        if (err) {
                            console.error(`❌ Error storing history for ${deviceId}:`, err.message);
                        } else {
                            console.log(`📜 ${deviceId} history entry #${this.lastID} created`);
                        }
                    }
                );
                
                // Update tank water level in database
                updateTankWaterLevel(enrichedData.tankId, enrichedData.waterLevel, enrichedData.percentage);
                
                // Broadcast to WebSocket clients with flow rate
                broadcastToAll({
                    type: 'tank_live_update',
                    deviceId: deviceId,
                    tankId: enrichedData.tankId,
                    data: enrichedData,
                    timestamp: Date.now()
                });
            }
        }
    });
}

function updateTankWaterLevel(tankId, waterLevel, percentage) {
    db.run(
        'UPDATE tanks SET waterLevel = ? WHERE tankId = ?',
        [waterLevel, tankId],
        function(err) {
            if (err) {
                console.error(`❌ Error updating tank ${tankId}:`, err);
            } else if (this.changes > 0) {
                console.log(`✅ Updated tank ${tankId} water level to ${waterLevel}m`);
                
                // Broadcast update via WebSocket
                broadcastToAll({
                    type: 'tank_updated',
                    tankId: tankId,
                    waterLevel: waterLevel,
                    percentage: percentage,
                    timestamp: Date.now()
                });
            }
        }
    );
}

function startListeningToAllDevices() {
    if (Object.keys(TANK_PARAMETERS).length === 0) {
        console.log('⚠️ No devices to monitor. Waiting for parameters...');
        return;
    }
    
    console.log('🚀 Starting Firebase device monitoring...\n');
    
    Object.keys(TANK_PARAMETERS).forEach(deviceId => {
        const params = TANK_PARAMETERS[deviceId];
        
        const requiredParams = ['height', 'sensorHeight', 'diameter', 'capacity', 'shape'];
        const missingParams = requiredParams.filter(param => !params[param]);
        
        if (missingParams.length > 0) {
            console.warn(`⚠️ Skipping ${deviceId} - Missing: ${missingParams.join(', ')}`);
            return;
        }
        
        startListeningToDevice(deviceId);
    });
}

async function initializeFirebaseSync() {
    console.log('\n' + '='.repeat(60));
    console.log('🔥 Initializing Firebase Tank Monitoring System');
    console.log('='.repeat(60));
    
    try {
        await syncTankParametersToFirebase();
        await loadTankParameters();
        watchParameterChanges();
        startListeningToAllDevices();
        
        console.log('✅ Firebase integration ready!');
        console.log('='.repeat(60) + '\n');
    } catch (error) {
        console.error('❌ Firebase initialization failed:', error);
    }
}

// ==================== API ENDPOINTS ====================

// GET /api/poll/all
app.get('/api/poll/all', async (req, res) => {
    try {
        const tanks = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM tanks', [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows.map(r => ({ ...r, isActive: r.isActive === 1 })));
            });
        });
        
        const valves = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM gate_valves', [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows.map(r => ({ ...r, isOpen: r.isOpen === 1 })));
            });
        });
        
        const pipelines = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM pipelines WHERE active = 1', [], (err, rows) => {
                if (err) reject(err);
                else {
                    // Parse nodes for each pipeline
                    const parsed = rows.map(row => {
                        try {
                            return {
                                ...row,
                                nodes: typeof row.nodes === 'string' ? JSON.parse(row.nodes) : row.nodes
                            };
                        } catch (parseError) {
                            console.error(`Error parsing nodes for pipeline ${row.id}:`, parseError);
                            return {
                                ...row,
                                nodes: []
                            };
                        }
                    });
                    resolve(parsed);
                }
            });
        });
        
        res.json({
            success: true,
            timestamp: Date.now(),
            tanks,
            valves,
            pipelines
        });
        
    } catch (error) {
        console.error('Poll error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ==================== TANKS ENDPOINTS ====================

// POST /api/tank
app.post('/api/tank', async (req, res) => {
    const tank = req.body;
    
    if (!tank.tankId || !tank.name || !tank.latitude || !tank.longitude || !tank.capacity) {
        return res.status(400).json({ error: 'Missing required fields' });
    }
    
    db.run(
        `INSERT INTO tanks (tankId, deviceId, name, state, district, mandal, habitation, latitude, longitude, type, shape, diameter, height, sensorHeight, capacity, waterLevel, isActive)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [tank.tankId, tank.deviceId || '', tank.name, tank.state || '', tank.district || '', tank.mandal || '', tank.habitation || '',
         tank.latitude, tank.longitude, tank.type || 'OHSR', tank.shape || 'cylinder', tank.diameter || 5.0, tank.height || 10.0,
         tank.sensorHeight || 10.0, tank.capacity, tank.waterLevel || 8.5, tank.isActive ? 1 : 0],
        async function(err) {
            if (err) return res.status(500).json({ error: err.message });
            
            if (tank.deviceId && tank.isActive) {
                try {
                    await syncTankParametersToFirebase();
                    await loadTankParameters();
                    if (TANK_PARAMETERS[tank.deviceId]) {
                        startListeningToDevice(tank.deviceId);
                    }
                } catch (error) {
                    console.error('Error syncing new tank:', error);
                }
            }
            
            res.json({ tankId: tank.tankId, message: 'Tank added' });
            
            // Broadcast update
            broadcastToAll({
                type: 'tank_added',
                tankId: tank.tankId,
                name: tank.name,
                timestamp: Date.now()
            });
        }
    );
});

// GET /api/tanks
app.get('/api/tanks', (req, res) => {
    db.all('SELECT * FROM tanks ORDER BY created_at DESC', [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        const tanks = rows.map(row => ({ ...row, isActive: row.isActive === 1 }));
        res.json(tanks);
    });
});

// GET /api/tank/:tankId
app.get('/api/tank/:tankId', (req, res) => {
    db.get('SELECT * FROM tanks WHERE tankId = ?', [req.params.tankId], (err, row) => {
        if (err) return res.status(500).json({ error: err.message });
        if (!row) return res.status(404).json({ error: 'Tank not found' });
        row.isActive = row.isActive === 1;
        res.json(row);
    });
});

// PUT /api/tank/:tankId
app.put('/api/tank/:tankId', async (req, res) => {
    const tank = req.body;
    const updates = [];
    const values = [];
    
    Object.keys(tank).forEach(key => {
        if (key !== 'tankId') {
            updates.push(`${key} = ?`);
            values.push(key === 'isActive' ? (tank[key] ? 1 : 0) : tank[key]);
        }
    });
    
    if (updates.length === 0) return res.status(400).json({ error: 'No fields to update' });
    values.push(req.params.tankId);
    
    db.run(`UPDATE tanks SET ${updates.join(', ')} WHERE tankId = ?`, values, async function(err) {
        if (err) return res.status(500).json({ error: err.message });
        
        if (tank.deviceId !== undefined || tank.isActive !== undefined) {
            try {
                await syncTankParametersToFirebase();
                await loadTankParameters();
            } catch (error) {
                console.error('Error re-syncing tank:', error);
            }
        }
        
        res.json({ message: 'Tank updated', changes: this.changes });
        
        // Broadcast update
        broadcastToAll({
            type: 'tank_updated',
            tankId: req.params.tankId,
            changes: Object.keys(tank),
            timestamp: Date.now()
        });
    });
});

// DELETE /api/tank/:tankId
app.delete('/api/tank/:tankId', (req, res) => {
    db.run('DELETE FROM tanks WHERE tankId = ?', [req.params.tankId], function(err) {
        if (err) return res.status(500).json({ error: err.message });
        
        res.json({ message: 'Tank deleted', changes: this.changes });
        
        // Broadcast update
        broadcastToAll({
            type: 'tank_deleted',
            tankId: req.params.tankId,
            timestamp: Date.now()
        });
    });
});

// ==================== VALVES ENDPOINTS ====================

// POST /api/valve
app.post('/api/valve', (req, res) => {
    const valve = req.body;
    
    if (!valve.valveId || !valve.name || !valve.latitude || !valve.longitude || valve.households === undefined) {
        return res.status(400).json({ error: 'Missing required fields' });
    }
    
    db.run(
        `INSERT INTO gate_valves (valveId, name, type, category, parentValveId, households, flowRate, mandal, habitation, latitude, longitude, isOpen)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [valve.valveId, valve.name, valve.type || 'STRAIGHT', valve.category || 'main', valve.parentValveId || null,
         valve.households || 0, valve.flowRate || 0, valve.mandal || '', valve.habitation || '',
         valve.latitude, valve.longitude, valve.isOpen ? 1 : 0],
        function(err) {
            if (err) return res.status(500).json({ error: err.message });
            
            res.json({ valveId: valve.valveId, message: 'Valve added' });
            
            // Store initial history
            db.run(
                `INSERT INTO valve_history (valveId, name, isOpen, flowRate, pressure, timestamp)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                [valve.valveId, valve.name, valve.isOpen ? 1 : 0, valve.flowRate || 0, 0, Date.now()],
                (err) => {
                    if (err) console.error('Error storing valve history:', err);
                }
            );
            
            // Broadcast update
            broadcastToAll({
                type: 'valve_added',
                valveId: valve.valveId,
                name: valve.name,
                timestamp: Date.now()
            });
        }
    );
});

// GET /api/valves
app.get('/api/valves', (req, res) => {
    db.all('SELECT * FROM gate_valves ORDER BY created_at DESC', [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        const valves = rows.map(row => ({ ...row, isOpen: row.isOpen === 1 }));
        res.json(valves);
    });
});

// GET /api/valve/:valveId
app.get('/api/valve/:valveId', (req, res) => {
    db.get('SELECT * FROM gate_valves WHERE valveId = ?', [req.params.valveId], (err, row) => {
        if (err) return res.status(500).json({ error: err.message });
        if (!row) return res.status(404).json({ error: 'Valve not found' });
        row.isOpen = row.isOpen === 1;
        res.json(row);
    });
});

// PUT /api/valve/:valveId
app.put('/api/valve/:valveId', (req, res) => {
    const valve = req.body;
    const updates = [];
    const values = [];
    
    Object.keys(valve).forEach(key => {
        if (key !== 'valveId') {
            updates.push(`${key} = ?`);
            values.push(key === 'isOpen' ? (valve[key] ? 1 : 0) : valve[key]);
        }
    });
    
    if (updates.length === 0) return res.status(400).json({ error: 'No fields to update' });
    values.push(req.params.valveId);
    
    db.run(`UPDATE gate_valves SET ${updates.join(', ')} WHERE valveId = ?`, values, function(err) {
        if (err) return res.status(500).json({ error: err.message });
        
        // Store history for valve state changes
        if (valve.isOpen !== undefined) {
            db.run(
                `INSERT INTO valve_history (valveId, name, isOpen, flowRate, pressure, timestamp)
                 VALUES (?, (SELECT name FROM gate_valves WHERE valveId = ?), ?, ?, ?, ?)`,
                [req.params.valveId, req.params.valveId, valve.isOpen ? 1 : 0,
                 valve.flowRate || 0, 0, Date.now()],
                (err) => {
                    if (err) console.error('Error storing valve history:', err);
                }
            );
        }
        
        res.json({ message: 'Valve updated', changes: this.changes });
        
        // Broadcast update
        broadcastToAll({
            type: 'valve_updated',
            valveId: req.params.valveId,
            changes: Object.keys(valve),
            timestamp: Date.now()
        });
    });
});

// DELETE /api/valve/:valveId
app.delete('/api/valve/:valveId', (req, res) => {
    // First, update any sub-valves to remove parent reference
    db.run('UPDATE gate_valves SET parentValveId = NULL WHERE parentValveId = ?', [req.params.valveId], (err) => {
        if (err) return res.status(500).json({ error: err.message });
        
        // Then delete the valve
        db.run('DELETE FROM gate_valves WHERE valveId = ?', [req.params.valveId], function(delErr) {
            if (delErr) return res.status(500).json({ error: delErr.message });
            
            res.json({ message: 'Valve deleted', changes: this.changes });
            
            // Broadcast update
            broadcastToAll({
                type: 'valve_deleted',
                valveId: req.params.valveId,
                timestamp: Date.now()
            });
        });
    });
});

// PATCH /api/valve/:valveId/toggle
app.patch('/api/valve/:valveId/toggle', (req, res) => {
    db.get('SELECT isOpen, name FROM gate_valves WHERE valveId = ?', [req.params.valveId], (err, row) => {
        if (err) return res.status(500).json({ error: err.message });
        if (!row) return res.status(404).json({ error: 'Valve not found' });
        
        const newState = row.isOpen === 1 ? 0 : 1;
        
        db.run('UPDATE gate_valves SET isOpen = ? WHERE valveId = ?', [newState, req.params.valveId], function(err) {
            if (err) return res.status(500).json({ error: err.message });
            
            // Store history
            db.run(
                `INSERT INTO valve_history (valveId, name, isOpen, flowRate, pressure, timestamp)
                 VALUES (?, ?, ?, (SELECT flowRate FROM gate_valves WHERE valveId = ?), 0, ?)`,
                [req.params.valveId, row.name, newState, req.params.valveId, Date.now()],
                (err) => {
                    if (err) console.error('Error storing valve history:', err);
                }
            );
            
            res.json({ message: 'Valve toggled', isOpen: newState === 1, valveId: req.params.valveId });
            
            // Broadcast update
            broadcastToAll({
                type: 'valve_toggled',
                valveId: req.params.valveId,
                isOpen: newState === 1,
                timestamp: Date.now()
            });
        });
    });
});

// ==================== PIPELINES ENDPOINTS ====================

// POST /api/pipeline
app.post('/api/pipeline', (req, res) => {
    const pipeline = req.body;
    
    if (!pipeline.nodes || !Array.isArray(pipeline.nodes)) {
        return res.status(400).json({ error: 'Nodes array is required' });
    }
    
    db.run(
        `INSERT INTO pipelines (name, type, diameter, capacity, startPoint, endPoint, notes, nodes, active)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [pipeline.name || `Pipeline ${Date.now()}`, pipeline.type || 'PVC', pipeline.diameter || 150,
         pipeline.capacity || 500, pipeline.startPoint || '', pipeline.endPoint || '',
         pipeline.notes || '', JSON.stringify(pipeline.nodes), pipeline.active !== false ? 1 : 0],
        function(err) {
            if (err) return res.status(500).json({ error: err.message });
            
            res.json({ id: this.lastID, message: 'Pipeline saved' });
            
            // Broadcast update
            broadcastToAll({
                type: 'pipeline_added',
                pipelineId: this.lastID,
                name: pipeline.name,
                timestamp: Date.now()
            });
        }
    );
});

// GET /api/pipelines
app.get('/api/pipelines', (req, res) => {
    db.all('SELECT * FROM pipelines WHERE active = 1 ORDER BY created_at DESC', [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        
        // Parse the nodes JSON string for each pipeline
        const pipelines = rows.map(row => {
            try {
                return {
                    ...row,
                    nodes: typeof row.nodes === 'string' ? JSON.parse(row.nodes) : row.nodes
                };
            } catch (parseError) {
                console.error(`Error parsing nodes for pipeline ${row.id}:`, parseError);
                return {
                    ...row,
                    nodes: []
                };
            }
        });
        
        res.json(pipelines);
    });
});

// GET /api/pipeline/:id
app.get('/api/pipeline/:id', (req, res) => {
    db.get('SELECT * FROM pipelines WHERE id = ? AND active = 1', [req.params.id], (err, row) => {
        if (err) return res.status(500).json({ error: err.message });
        if (!row) return res.status(404).json({ error: 'Pipeline not found' });
        
        // Parse the nodes JSON string
        try {
            row.nodes = typeof row.nodes === 'string' ? JSON.parse(row.nodes) : row.nodes;
        } catch (parseError) {
            console.error(`Error parsing nodes for pipeline ${row.id}:`, parseError);
            row.nodes = [];
        }
        
        res.json(row);
    });
});

// PUT /api/pipeline/:id
app.put('/api/pipeline/:id', (req, res) => {
    const pipeline = req.body;
    
    if (!pipeline.name && !pipeline.type && !pipeline.diameter && !pipeline.capacity && 
        !pipeline.startPoint && !pipeline.endPoint && !pipeline.notes && !pipeline.nodes) {
        return res.status(400).json({ error: 'No fields to update' });
    }
    
    const updates = [];
    const values = [];
    
    if (pipeline.name) { updates.push('name = ?'); values.push(pipeline.name); }
    if (pipeline.type) { updates.push('type = ?'); values.push(pipeline.type); }
    if (pipeline.diameter) { updates.push('diameter = ?'); values.push(pipeline.diameter); }
    if (pipeline.capacity) { updates.push('capacity = ?'); values.push(pipeline.capacity); }
    if (pipeline.startPoint) { updates.push('startPoint = ?'); values.push(pipeline.startPoint); }
    if (pipeline.endPoint) { updates.push('endPoint = ?'); values.push(pipeline.endPoint); }
    if (pipeline.notes !== undefined) { updates.push('notes = ?'); values.push(pipeline.notes); }
    if (pipeline.nodes) { updates.push('nodes = ?'); values.push(JSON.stringify(pipeline.nodes)); }
    if (pipeline.active !== undefined) { updates.push('active = ?'); values.push(pipeline.active ? 1 : 0); }
    
    values.push(req.params.id);
    
    db.run(`UPDATE pipelines SET ${updates.join(', ')} WHERE id = ?`, values, function(err) {
        if (err) return res.status(500).json({ error: err.message });
        
        res.json({ message: 'Pipeline updated', changes: this.changes });
        
        // Broadcast update
        broadcastToAll({
            type: 'pipeline_updated',
            pipelineId: req.params.id,
            changes: updates,
            timestamp: Date.now()
        });
    });
});

// DELETE /api/pipeline/:id
app.delete('/api/pipeline/:id', (req, res) => {
    // Soft delete by setting active = 0
    db.run('UPDATE pipelines SET active = 0 WHERE id = ?', [req.params.id], function(err) {
        if (err) return res.status(500).json({ error: err.message });
        
        res.json({ message: 'Pipeline deleted', changes: this.changes });
        
        // Broadcast update
        broadcastToAll({
            type: 'pipeline_deleted',
            pipelineId: req.params.id,
            timestamp: Date.now()
        });
    });
});

// ==================== HISTORY ENDPOINTS ====================

// GET /api/history/:deviceId
app.get('/api/history/:deviceId', (req, res) => {
    const { type = 'tanks', limit = 100, offset = 0 } = req.query;
    
    if (type === 'tanks') {
        db.all(
            `SELECT * FROM tank_history WHERE deviceId = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`,
            [req.params.deviceId, parseInt(limit), parseInt(offset)],
            (err, rows) => {
                if (err) return res.status(500).json({ error: err.message });
                res.json({ deviceId: req.params.deviceId, count: rows.length, history: rows });
            }
        );
    } else if (type === 'valves') {
        db.all(
            `SELECT * FROM valve_history WHERE valveId = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`,
            [req.params.deviceId, parseInt(limit), parseInt(offset)],
            (err, rows) => {
                if (err) return res.status(500).json({ error: err.message });
                res.json({ deviceId: req.params.deviceId, count: rows.length, history: rows });
            }
        );
    } else {
        return res.status(400).json({ error: 'Invalid type parameter' });
    }
});

// GET /api/history/:deviceId/latest
app.get('/api/history/:deviceId/latest', (req, res) => {
    const { type = 'tanks' } = req.query;
    
    if (type === 'tanks') {
        db.get(
            `SELECT * FROM tank_history WHERE deviceId = ? ORDER BY timestamp DESC LIMIT 1`,
            [req.params.deviceId],
            (err, row) => {
                if (err) return res.status(500).json({ error: err.message });
                if (!row) return res.status(404).json({ error: 'No history found' });
                res.json(row);
            }
        );
    } else if (type === 'valves') {
        db.get(
            `SELECT * FROM valve_history WHERE valveId = ? ORDER BY timestamp DESC LIMIT 1`,
            [req.params.deviceId],
            (err, row) => {
                if (err) return res.status(500).json({ error: err.message });
                if (!row) return res.status(404).json({ error: 'No history found' });
                res.json(row);
            }
        );
    } else {
        return res.status(400).json({ error: 'Invalid type parameter' });
    }
});

// GET /api/history/:deviceId/range
app.get('/api/history/:deviceId/range', (req, res) => {
    const { type = 'tanks', start, end } = req.query;
    
    if (!start || !end) {
        return res.status(400).json({ error: 'start and end timestamps required' });
    }
    
    if (type === 'tanks') {
        db.all(
            `SELECT * FROM tank_history WHERE deviceId = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp DESC`,
            [req.params.deviceId, parseInt(start), parseInt(end)],
            (err, rows) => {
                if (err) return res.status(500).json({ error: err.message });
                res.json({ deviceId: req.params.deviceId, start, end, count: rows.length, history: rows });
            }
        );
    } else if (type === 'valves') {
        db.all(
            `SELECT * FROM valve_history WHERE valveId = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp DESC`,
            [req.params.deviceId, parseInt(start), parseInt(end)],
            (err, rows) => {
                if (err) return res.status(500).json({ error: err.message });
                res.json({ deviceId: req.params.deviceId, start, end, count: rows.length, history: rows });
            }
        );
    } else {
        return res.status(400).json({ error: 'Invalid type parameter' });
    }
});

// DELETE /api/history/:deviceId
app.delete('/api/history/:deviceId', (req, res) => {
    const { type = 'tanks' } = req.query;
    
    if (type === 'tanks') {
        db.run('DELETE FROM tank_history WHERE deviceId = ?', [req.params.deviceId], function(err) {
            if (err) return res.status(500).json({ error: err.message });
            res.json({ message: 'History deleted', deleted: this.changes });
        });
    } else if (type === 'valves') {
        db.run('DELETE FROM valve_history WHERE valveId = ?', [req.params.deviceId], function(err) {
            if (err) return res.status(500).json({ error: err.message });
            res.json({ message: 'History deleted', deleted: this.changes });
        });
    } else {
        return res.status(400).json({ error: 'Invalid type parameter' });
    }
});

// ==================== SENSOR DATA ENDPOINTS ====================

// GET /api/sensor/:deviceId
app.get('/api/sensor/:deviceId', (req, res) => {
    db.get('SELECT * FROM sensor_cache WHERE deviceId = ?', [req.params.deviceId], (err, row) => {
        if (err) return res.status(500).json({ error: err.message });
        if (!row) return res.status(404).json({ error: 'No sensor data found' });
        res.json(row);
    });
});

// GET /api/sensor/:deviceId/live
app.get('/api/sensor/:deviceId/live', (req, res) => {
    db.get('SELECT * FROM sensor_cache WHERE deviceId = ?', [req.params.deviceId], (err, row) => {
        if (err) return res.status(500).json({ error: err.message });
        
        if (row) {
            res.json({
                waterLevel: row.waterLevel,
                volume: row.volume,
                percentage: row.percentage,
                pressure: row.pressure,
                weight: row.weight,
                status: row.status,
                lastUpdated: row.lastUpdated
            });
        } else {
            // Check Firebase for live data
            const deviceRef = ref(firebaseDb, `tanks/${req.params.deviceId}`);
            get(deviceRef).then(snapshot => {
                if (snapshot.exists()) {
                    res.json(snapshot.val());
                } else {
                    res.status(404).json({ error: 'No live sensor data found' });
                }
            }).catch(error => {
                res.status(500).json({ error: error.message });
            });
        }
    });
});

// ==================== FLOW ENDPOINTS ====================

// GET /api/flow/calculate
app.get('/api/flow/calculate', async (req, res) => {
    try {
        const tanks = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM tanks', [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows.map(r => ({ ...r, isActive: r.isActive === 1 })));
            });
        });
        
        const valves = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM gate_valves', [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows.map(r => ({ ...r, isOpen: r.isOpen === 1 })));
            });
        });
        
        const pipelines = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM pipelines WHERE active = 1', [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });
        
        const flowData = calculateFlowPaths(tanks, valves, pipelines);
        res.json(flowData);
        
    } catch (error) {
        console.error('Flow calculation error:', error);
        res.status(500).json({ error: error.message });
    }
});

// ==================== SUPPLY ENDPOINTS ====================

// GET /api/supply/overview
app.get('/api/supply/overview', async (req, res) => {
    try {
        const tanks = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM tanks', [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows.map(r => ({ ...r, isActive: r.isActive === 1 })));
            });
        });
        
        const valves = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM gate_valves', [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows.map(r => ({ ...r, isOpen: r.isOpen === 1 })));
            });
        });
        
        const pipelines = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM pipelines WHERE active = 1', [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });
        
        const supplyOverview = calculateSupplyOverview(tanks, valves, pipelines);
        res.json(supplyOverview);
        
    } catch (error) {
        console.error('Supply overview error:', error);
        res.status(500).json({ error: error.message });
    }
});

// ==================== EXPORT/IMPORT ENDPOINTS ====================

// GET /api/export/all
app.get('/api/export/all', async (req, res) => {
    try {
        const tanks = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM tanks', [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows.map(r => ({ ...r, isActive: r.isActive === 1 })));
            });
        });
        
        const valves = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM gate_valves', [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows.map(r => ({ ...r, isOpen: r.isOpen === 1 })));
            });
        });
        
        const pipelines = await new Promise((resolve, reject) => {
            db.all('SELECT * FROM pipelines WHERE active = 1', [], (err, rows) => {
                if (err) reject(err);
                else {
                    // Parse nodes for each pipeline
                    const parsed = rows.map(row => {
                        try {
                            return {
                                ...row,
                                nodes: typeof row.nodes === 'string' ? JSON.parse(row.nodes) : row.nodes
                            };
                        } catch (parseError) {
                            console.error(`Error parsing nodes for pipeline ${row.id}:`, parseError);
                            return {
                                ...row,
                                nodes: []
                            };
                        }
                    });
                    resolve(parsed);
                }
            });
        });
        
        res.json({
            version: '1.0',
            timestamp: Date.now(),
            exportDate: new Date().toISOString(),
            tanks,
            valves,
            pipelines
        });
        
    } catch (error) {
        console.error('Export error:', error);
        res.status(500).json({ error: error.message });
    }
});

// POST /api/import
app.post('/api/import', async (req, res) => {
    const data = req.body;
    
    if (!data.tanks || !data.valves || !data.pipelines) {
        return res.status(400).json({ error: 'Invalid import data format' });
    }
    
    try {
        // Import tanks
        for (const tank of data.tanks) {
            await new Promise((resolve, reject) => {
                db.run(
                    `INSERT OR REPLACE INTO tanks (tankId, deviceId, name, state, district, mandal, habitation, latitude, longitude, type, shape, diameter, height, sensorHeight, capacity, waterLevel, isActive)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [tank.tankId, tank.deviceId || '', tank.name, tank.state || '', tank.district || '', tank.mandal || '', tank.habitation || '',
                     tank.latitude, tank.longitude, tank.type || 'OHSR', tank.shape || 'cylinder', tank.diameter || 5.0, tank.height || 10.0,
                     tank.sensorHeight || 10.0, tank.capacity, tank.waterLevel || 8.5, tank.isActive ? 1 : 0],
                    function(err) {
                        if (err) reject(err);
                        else resolve();
                    }
                );
            });
        }
        
        // Import valves
        for (const valve of data.valves) {
            await new Promise((resolve, reject) => {
                db.run(
                    `INSERT OR REPLACE INTO gate_valves (valveId, name, type, category, parentValveId, households, flowRate, mandal, habitation, latitude, longitude, isOpen)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [valve.valveId, valve.name, valve.type || 'STRAIGHT', valve.category || 'main', valve.parentValveId || null,
                     valve.households || 0, valve.flowRate || 0, valve.mandal || '', valve.habitation || '',
                     valve.latitude, valve.longitude, valve.isOpen ? 1 : 0],
                    function(err) {
                        if (err) reject(err);
                        else resolve();
                    }
                );
            });
        }
        
        // Import pipelines
        for (const pipeline of data.pipelines) {
            await new Promise((resolve, reject) => {
                db.run(
                    `INSERT OR REPLACE INTO pipelines (id, name, type, diameter, capacity, startPoint, endPoint, notes, nodes, active)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [pipeline.id, pipeline.name, pipeline.type || 'PVC', pipeline.diameter || 150,
                     pipeline.capacity || 500, pipeline.startPoint || '', pipeline.endPoint || '',
                     pipeline.notes || '', JSON.stringify(pipeline.nodes), pipeline.active !== false ? 1 : 0],
                    function(err) {
                        if (err) reject(err);
                        else resolve();
                    }
                );
            });
        }
        
        res.json({ message: 'Data imported successfully' });
        
        // Broadcast refresh
        broadcastToAll({
            type: 'data_refreshed',
            timestamp: Date.now()
        });
        
    } catch (error) {
        console.error('Import error:', error);
        res.status(500).json({ error: error.message });
    }
});

// ==================== ERROR HANDLING ====================

app.use((req, res) => {
    res.status(404).json({ error: `Cannot ${req.method} ${req.path}` });
});

app.use((err, req, res, next) => {
    console.error('❌ Server error:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
});

// ==================== WEBSOCKET SERVER ====================

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const wsClients = new Set();

wss.on('connection', (ws) => {
    console.log('🔌 New WebSocket client connected');
    wsClients.add(ws);
    
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    
    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            if (data.type === 'ping') {
                ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
            }
        } catch (err) {
            console.error('Error parsing WebSocket message:', err);
        }
    });
    
    ws.on('close', () => {
        console.log('🔌 WebSocket client disconnected');
        wsClients.delete(ws);
    });
    
    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
        wsClients.delete(ws);
    });
    
    ws.send(JSON.stringify({
        type: 'connected',
        message: 'Connected to Water Pipeline System',
        timestamp: Date.now()
    }));
});

function broadcastToAll(data) {
    if (wsClients.size === 0) return;
    
    const message = JSON.stringify(data);
    let sent = 0;
    
    wsClients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            try {
                client.send(message);
                sent++;
            } catch (err) {
                wsClients.delete(client);
            }
        } else {
            wsClients.delete(client);
        }
    });
}

setInterval(() => {
    wss.clients.forEach(ws => {
        if (ws.isAlive === false) {
            wsClients.delete(ws);
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

// ==================== CLEANUP ON EXIT ====================

process.on('SIGINT', () => {
    console.log('\n🛑 Shutting down server...');
    
    wsClients.forEach(client => {
        try {
            client.close(1000, 'Server shutting down');
        } catch (err) {
            console.error('Error closing client:', err);
        }
    });
    wss.close();
    
    db.close((err) => {
        if (err) {
            console.error('Error closing database:', err);
        } else {
            console.log('✅ Database closed');
        }
        process.exit(0);
    });
});

// ==================== START SERVER ====================

const PORT = 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log('='.repeat(60));
    console.log('🚀 Water Pipeline Management System');
    console.log('='.repeat(60));
    console.log('📡 WebSocket: ws://localhost:3000');
    console.log('🔥 Firebase: Real-time Tank Monitoring');
    console.log('🎯 Features: Pipeline, Tank & Valve Management');
    console.log('📊 Real-time Flow Calculations & Supply Overview');
    console.log('='.repeat(60));
    console.log('📋 ENDPOINTS SUMMARY:');
    console.log('   Tanks:');
    console.log('     GET  /api/tanks                    - List all tanks');
    console.log('     POST /api/tank                     - Create tank');
    console.log('     GET  /api/tank/:tankId             - Get tank details');
    console.log('     PUT  /api/tank/:tankId             - Update tank');
    console.log('     DELETE /api/tank/:tankId           - Delete tank');
    console.log('   Valves:');
    console.log('     GET  /api/valves                   - List all valves');
    console.log('     POST /api/valve                    - Create valve');
    console.log('     GET  /api/valve/:valveId           - Get valve details');
    console.log('     PUT  /api/valve/:valveId           - Update valve');
    console.log('     DELETE /api/valve/:valveId         - Delete valve');
    console.log('     PATCH /api/valve/:valveId/toggle   - Toggle valve');
    console.log('   Pipelines:');
    console.log('     GET  /api/pipelines                - List all pipelines');
    console.log('     POST /api/pipeline                 - Create pipeline');
    console.log('     GET  /api/pipeline/:id             - Get pipeline');
    console.log('     PUT  /api/pipeline/:id             - Update pipeline');
    console.log('     DELETE /api/pipeline/:id           - Delete pipeline');
    console.log('   History:');
    console.log('     GET  /api/history/:deviceId        - Get device history');
    console.log('     GET  /api/history/:deviceId/latest - Get latest reading');
    console.log('     GET  /api/history/:deviceId/range  - Get date range');
    console.log('     DELETE /api/history/:deviceId      - Delete history');
    console.log('   Sensor Data:');
    console.log('     GET  /api/sensor/:deviceId         - Get sensor data');
    console.log('     GET  /api/sensor/:deviceId/live    - Get live data');
    console.log('   Flow & Supply:');
    console.log('     GET  /api/flow/calculate           - Calculate flow paths');
    console.log('     GET  /api/supply/overview          - Get supply overview');
    console.log('   System:');
    console.log('     GET  /api/poll/all                - Poll all data');
    console.log('     GET  /api/export/all              - Export all data');
    console.log('     POST /api/import                  - Import data');
    console.log('='.repeat(60));
    console.log(`✅ Server running on http://0.0.0.0:${PORT}`);
    console.log('='.repeat(60));
});