import React, { useRef } from 'react';
import { Canvas, useFrame } from '@react-three/fiber';
import { OrbitControls, Float, Environment, Center } from '@react-three/drei';

function JusticeScales() {
  const groupRef = useRef();
  const beamRef = useRef();
  const leftPanRef = useRef();
  const rightPanRef = useRef();

  useFrame((state) => {
    const t = state.clock.getElapsedTime();
    
    // Very subtle idle floating sway (no spinning/rotating around)
    if (groupRef.current) {
        groupRef.current.rotation.y = Math.sin(t * 0.5) * 0.1; 
    }

    // Seesaw balance animation (One side up, other side down)
    const theta = Math.sin(t * 1.5) * 0.18; // tilt angle
    const beamLength = 2.5; // wider arm length from center

    if (beamRef.current) {
        beamRef.current.rotation.z = -theta;
    }

    const deltaY = beamLength * Math.sin(theta);
    const deltaX = beamLength * (1 - Math.cos(theta));

    if (leftPanRef.current) {
        leftPanRef.current.position.y = 1.3 - deltaY;
        leftPanRef.current.position.x = -beamLength + deltaX;
    }

    if (rightPanRef.current) {
        rightPanRef.current.position.y = 1.3 + deltaY;
        rightPanRef.current.position.x = beamLength - deltaX;
    }
  });

  const navyColor = "#051326";
  const goldColor = "#d4af37";

  return (
    <group ref={groupRef} scale={1.3}>
      {/* Base/Pedestal - Made slightly wider */}
      <mesh position={[0, -2, 0]}>
        <cylinderGeometry args={[1.8, 2.2, 0.4, 32]} />
        <meshStandardMaterial color={navyColor} metalness={0.5} roughness={0.2} />
      </mesh>
      <mesh position={[0, -1.6, 0]}>
        <cylinderGeometry args={[1.5, 1.8, 0.3, 32]} />
        <meshStandardMaterial color={navyColor} metalness={0.5} roughness={0.2} />
      </mesh>

      {/* Center Pillar */}
      <mesh position={[0, 0.5, 0]}>
        <cylinderGeometry args={[0.2, 0.35, 4, 32]} />
        <meshStandardMaterial color={navyColor} metalness={0.5} roughness={0.2} />
      </mesh>

      {/* Top Details */}
      <mesh position={[0, 2.6, 0]}>
        <cylinderGeometry args={[0.2, 0.3, 0.2, 32]} />
        <meshStandardMaterial color={goldColor} metalness={0.9} roughness={0.1} />
      </mesh>
      <mesh position={[0, 2.9, 0]}>
        <sphereGeometry args={[0.3, 32, 32]} />
        <meshStandardMaterial color={goldColor} metalness={0.9} roughness={0.1} />
      </mesh>

      {/* Balance Beam (The Scale Arm) */}
      <group position={[0, 2.3, 0]} ref={beamRef}>
        <mesh rotation={[0, 0, Math.PI / 2]}>
          <cylinderGeometry args={[0.1, 0.1, 5.2, 32]} />
          <meshStandardMaterial color={goldColor} metalness={0.9} roughness={0.1} />
        </mesh>
      </group>

      {/* Sub-group Left Pan */}
      <group ref={leftPanRef} position={[-2.5, 1.3, 0]}>
        {/* Strings supporting pan */}
        <mesh position={[-0.25, 0.5, 0]} rotation={[0, 0, 0.25]}>
          <cylinderGeometry args={[0.02, 0.02, 1.2, 8]} />
          <meshStandardMaterial color={goldColor} />
        </mesh>
        <mesh position={[0.25, 0.5, 0]} rotation={[0, 0, -0.25]}>
          <cylinderGeometry args={[0.02, 0.02, 1.2, 8]} />
          <meshStandardMaterial color={goldColor} />
        </mesh>
        <mesh position={[0, 0.5, 0.25]} rotation={[-0.25, 0, 0]}>
          <cylinderGeometry args={[0.02, 0.02, 1.2, 8]} />
          <meshStandardMaterial color={goldColor} />
        </mesh>
        <mesh position={[0, 0.5, -0.25]} rotation={[0.25, 0, 0]}>
          <cylinderGeometry args={[0.02, 0.02, 1.2, 8]} />
          <meshStandardMaterial color={goldColor} />
        </mesh>
        
        {/* Scale Pan Left */}
        <mesh position={[0, -0.1, 0]}>
          <cylinderGeometry args={[0.8, 0.6, 0.15, 32]} />
          <meshStandardMaterial color={goldColor} metalness={0.7} roughness={0.2} />
        </mesh>
      </group>

      {/* Sub-group Right Pan */}
      <group ref={rightPanRef} position={[2.5, 1.3, 0]}>
        {/* Strings supporting pan */}
        <mesh position={[-0.25, 0.5, 0]} rotation={[0, 0, 0.25]}>
          <cylinderGeometry args={[0.02, 0.02, 1.2, 8]} />
          <meshStandardMaterial color={goldColor} />
        </mesh>
        <mesh position={[0.25, 0.5, 0]} rotation={[0, 0, -0.25]}>
          <cylinderGeometry args={[0.02, 0.02, 1.2, 8]} />
          <meshStandardMaterial color={goldColor} />
        </mesh>
        <mesh position={[0, 0.5, 0.25]} rotation={[-0.25, 0, 0]}>
          <cylinderGeometry args={[0.02, 0.02, 1.2, 8]} />
          <meshStandardMaterial color={goldColor} />
        </mesh>
        <mesh position={[0, 0.5, -0.25]} rotation={[0.25, 0, 0]}>
          <cylinderGeometry args={[0.02, 0.02, 1.2, 8]} />
          <meshStandardMaterial color={goldColor} />
        </mesh>
        
        {/* Scale Pan Right */}
        <mesh position={[0, -0.1, 0]}>
          <cylinderGeometry args={[0.8, 0.6, 0.15, 32]} />
          <meshStandardMaterial color={goldColor} metalness={0.7} roughness={0.2} />
        </mesh>
      </group>

    </group>
  );
}

export default function Hero3DModel() {
  return (
    <div className="w-full h-full min-h-[400px] lg:min-h-[500px] relative z-20 cursor-grab active:cursor-grabbing">
      <Canvas camera={{ position: [0, 2, 9], fov: 45 }}>
        <ambientLight intensity={1.5} />
        <directionalLight position={[10, 10, 5]} intensity={2.5} castShadow />
        <directionalLight position={[-10, 10, -5]} intensity={1} />
        <Environment preset="city" />
        
        <Float 
          speed={2.5} 
          rotationIntensity={0.2} 
          floatIntensity={1.5} 
          floatingRange={[-0.2, 0.2]}
        >
          <Center>
            <JusticeScales />
          </Center>
        </Float>
        
        <OrbitControls 
          enableZoom={false} 
          enablePan={false}
          minPolarAngle={Math.PI / 2.5}
          maxPolarAngle={Math.PI / 2.1}
          autoRotate={false}
        />
      </Canvas>
    </div>
  );
}
