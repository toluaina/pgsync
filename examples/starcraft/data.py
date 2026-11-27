"""Sample data for the starcraft example."""

import typing as t

from sqlalchemy.orm import Session

from .models import Specie, Structure, Unit

PROTOSS: int = 1
ZERG: int = 2
TERRAN: int = 3

SPECIES: t.Dict[int, str] = {
    PROTOSS: "Protoss",
    ZERG: "Zerg",
    TERRAN: "Terran",
}

# (name, details) for each specie
UNITS: t.Dict[int, t.List[t.Tuple[str, str]]] = {
    PROTOSS: [
        (
            "Archon",
            "Created by merging two templar units, the archon is a powerful "
            "melee unit with a very durable force shield and a strong "
            "energy-based attack.",
        ),
        (
            "Carrier",
            "A powerful air unit. Carriers do not have their own attacks but "
            "create interceptors to fight for them.",
        ),
        (
            "Colossus",
            "The large quad-legged vehicle fires lasers in a splash pattern "
            "well-suited to destroying swarms of weaker units. This unit can "
            "also traverse differences in terrain height due to its long "
            "legs, and will appear to step over ledges and other obstacles "
            "due to the inverse kinematics system.",
        ),
        ("Dark Templar", "A permanently cloaked stealth warrior."),
        (
            "High Templar",
            "A physically fragile unit with strong psychic abilities.",
        ),
        ("Probe", "Worker that gathers resources and warps in structures."),
        ("Zealot", "Melee infantry that can charge at nearby enemies."),
        ("Stalker", "Ranged ground unit that can blink a short distance."),
        (
            "Sentry",
            "Support unit that makes force fields and guardian shields.",
        ),
        ("Adept", "Light infantry that sends a shade ahead to reposition."),
        ("Immortal", "Heavy walker with a barrier against large attacks."),
        (
            "Disruptor",
            "Robotic unit that fires a slow, deadly purification nova.",
        ),
        ("Observer", "Cloaked flying scout that detects hidden units."),
        ("Warp Prism", "Transport that can also act as a mobile power field."),
        (
            "Phoenix",
            "Fast air fighter that can lift ground units into the air.",
        ),
        (
            "Void Ray",
            "Air unit with a beam that is strong against armored targets.",
        ),
        (
            "Oracle",
            "Air harasser that can reveal an area and place stasis wards.",
        ),
        ("Tempest", "Capital ship with a very long attack range."),
        ("Mothership", "Unique flagship that cloaks nearby friendly units."),
    ],
    ZERG: [
        (
            "Zergling",
            "Fast but weak melee attacker ideal for swarming attacks in large "
            "numbers.",
        ),
        (
            "Larva",
            "The core genus of the zerg, larvae can mutate into other zerg "
            "breeds.",
        ),
        (
            "Drone",
            "Worker that gathers resources and mutates into structures.",
        ),
        ("Overlord", "Flying unit that gives control (supply) to the swarm."),
        ("Overseer", "Overlord evolution that detects cloaked units."),
        ("Queen", "Defensive caster that spawns larvae and heals allies."),
        ("Baneling", "Zergling evolution that explodes in acid on contact."),
        ("Roach", "Armored ranged unit that regenerates fast when burrowed."),
        ("Ravager", "Roach evolution that launches corrosive bile."),
        ("Hydralisk", "Ranged unit that can attack ground and air targets."),
        (
            "Lurker",
            "Hydralisk evolution that attacks in a line when burrowed.",
        ),
        (
            "Infestor",
            "Caster that can take control of enemy units for a time.",
        ),
        ("Swarm Host", "Siege unit that sends waves of temporary locusts."),
        (
            "Mutalisk",
            "Fast air unit with an attack that bounces between targets.",
        ),
        ("Corruptor", "Air unit that is strong against massive air targets."),
        ("Brood Lord", "Corruptor evolution that attacks ground from range."),
        ("Viper", "Air caster that can pull units and block enemy vision."),
        ("Ultralisk", "Massive melee unit with a cleaving attack."),
    ],
    TERRAN: [
        (
            "SCV",
            "The builder and resource gatherer of the terran race. Its Repair "
            'ability can be set to "autocast".',
        ),
        (
            "Marine",
            "The basic terran infantry, able to upgrade hit points with a "
            "shield.",
        ),
        ("Reaper", "Fast raider that can jump up and down cliffs."),
        ("Marauder", "Armored infantry with shots that slow enemies."),
        ("Ghost", "Cloaking sniper that can call down nuclear strikes."),
        ("Hellion", "Fast vehicle with a flame attack in a line."),
        (
            "Widow Mine",
            "Burrowed mine that fires a missile at nearby enemies.",
        ),
        ("Siege Tank", "Vehicle that changes to siege mode for long range."),
        ("Cyclone", "Mobile vehicle that locks on to a target and fires."),
        ("Thor", "Heavy walker that attacks ground and air units."),
        ("Viking", "Fighter that changes between air and ground modes."),
        ("Medivac", "Air transport that heals biological units."),
        ("Liberator", "Air gunship that controls a zone on the ground."),
        ("Raven", "Air support unit that detects cloaked units."),
        ("Banshee", "Air attack craft that can cloak."),
        ("Battlecruiser", "Capital ship that can warp across the map."),
    ],
}

# (name, details) for each specie
STRUCTURES: t.Dict[int, t.List[t.Tuple[str, str]]] = {
    PROTOSS: [
        (
            "Assimilator",
            "Allows probes to harvest vespene gas from geysers.",
        ),
        ("Nexus", "Main base. Trains probes and receives gathered resources."),
        ("Pylon", "Gives supply and a power field for other structures."),
        ("Gateway", "Warps in ground infantry units."),
        ("Forge", "Researches ground weapon, armor, and shield upgrades."),
        ("Cybernetics Core", "Unlocks advanced gateway units and research."),
        (
            "Photon Cannon",
            "Defensive structure that also detects cloaked units.",
        ),
        ("Shield Battery", "Restores shields of nearby units and structures."),
        ("Twilight Council", "Researches charge, blink, and other upgrades."),
        ("Stargate", "Builds protoss air units."),
        ("Robotics Facility", "Builds robotic ground units and observers."),
        ("Robotics Bay", "Unlocks the colossus and the disruptor."),
        ("Templar Archives", "Unlocks the high templar and psionic storm."),
        ("Dark Shrine", "Unlocks the dark templar."),
        ("Fleet Beacon", "Unlocks capital ships and air upgrades."),
    ],
    ZERG: [
        (
            "Baneling Nest",
            "Required for baneling production and researches baneling "
            "upgrades.",
        ),
        ("Hatchery", "Main base. Makes larvae and receives resources."),
        ("Lair", "Hatchery evolution that unlocks mid-game tech."),
        ("Hive", "Lair evolution that unlocks late-game tech."),
        ("Extractor", "Allows drones to harvest vespene gas from geysers."),
        ("Spawning Pool", "Unlocks zerglings and queens."),
        (
            "Evolution Chamber",
            "Researches ground attack and carapace upgrades.",
        ),
        ("Roach Warren", "Unlocks roaches and roach upgrades."),
        ("Hydralisk Den", "Unlocks hydralisks and hydralisk upgrades."),
        ("Lurker Den", "Unlocks the lurker evolution."),
        ("Infestation Pit", "Unlocks infestors and swarm hosts."),
        ("Spire", "Unlocks mutalisks and corruptors."),
        ("Greater Spire", "Spire evolution that unlocks brood lords."),
        ("Ultralisk Cavern", "Unlocks ultralisks."),
        ("Nydus Network", "Makes tunnels to move units across the map."),
        ("Spine Crawler", "Defensive structure that attacks ground units."),
        ("Spore Crawler", "Defensive structure that attacks air and detects."),
    ],
    TERRAN: [
        ("Barracks", "Produces terran infantry units."),
        ("Command Center", "Main base. Trains SCVs and receives resources."),
        ("Orbital Command", "Command center upgrade that calls down MULEs."),
        ("Planetary Fortress", "Command center upgrade with a strong cannon."),
        ("Supply Depot", "Gives supply and can lower to let units pass."),
        ("Refinery", "Allows SCVs to harvest vespene gas from geysers."),
        ("Engineering Bay", "Researches infantry weapon and armor upgrades."),
        ("Bunker", "Defensive structure that holds infantry."),
        (
            "Missile Turret",
            "Defensive structure that attacks air and detects.",
        ),
        ("Sensor Tower", "Shows enemy movement in a large radius."),
        ("Factory", "Produces terran vehicles."),
        ("Starport", "Produces terran air units."),
        ("Armory", "Researches vehicle and ship upgrades."),
        ("Ghost Academy", "Unlocks ghosts and builds nukes."),
        ("Fusion Core", "Unlocks battlecruisers and advanced ship upgrades."),
    ],
}


def seed(session: Session) -> None:
    session.add_all(
        Specie(id=specie_id, name=name) for specie_id, name in SPECIES.items()
    )
    session.flush()
    session.add_all(
        Unit(name=name, details=details, specie_id=specie_id)
        for specie_id, rows in UNITS.items()
        for name, details in rows
    )
    session.flush()
    session.add_all(
        Structure(name=name, details=details, specie_id=specie_id)
        for specie_id, rows in STRUCTURES.items()
        for name, details in rows
    )
