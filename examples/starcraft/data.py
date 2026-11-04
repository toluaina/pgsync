"""Sample data for the starcraft example."""

import typing as t

from sqlalchemy.orm import Session

from .models import Specie, Structure, Unit


def seed(session: Session) -> None:
    species: t.List[Specie] = [
        Specie(id=1, name="Protos"),
        Specie(id=2, name="Zerg"),
        Specie(id=3, name="Terran"),
    ]
    session.add_all(species)
    session.flush()
    units: t.List[Unit] = [
        Unit(
            name="Archon",
            details="Created by merging two templar units, the archon is a powerful melee unit with a very durable force shield and a strong energy-based attack.",
            specie_id=1,
        ),
        Unit(
            name="Carrier",
            details="A powerful air unit. Carriers do not have their own attacks but create interceptors to fight for them.",
            specie_id=1,
        ),
        Unit(
            name="Colossus",
            details="The large quad-legged vehicle fires lasers in a splash pattern well-suited to destroying swarms of weaker units. This unit can also traverse differences in terrain height due to its long legs, and will appear to step over ledges and other obstacles due to the inverse kinematics system.",
            specie_id=1,
        ),
        Unit(
            name="Dark Templar",
            details="A permanently cloaked stealth warrior.",
            specie_id=1,
        ),
        Unit(
            name="High Templar",
            details="A physically fragile unit with strong psychic abilities.",
            specie_id=1,
        ),
        Unit(
            name="Zergling",
            details="Fast but weak melee attacker ideal for swarming attacks in large numbers.",
            specie_id=2,
        ),
        Unit(
            name="Larva",
            details="The core genus of the zerg, larvae can mutate into other zerg breeds.",
            specie_id=2,
        ),
        Unit(
            name="SCV",
            details='The builder and resource gatherer of the terran race. Its Repair ability can be set to "autocast".',
            specie_id=3,
        ),
        Unit(
            name="Marine",
            details="The basic terran infantry, able to upgrade hit points with a shield.",
            specie_id=3,
        ),
    ]
    session.add_all(units)
    session.flush()
    structures: t.List[Structure] = [
        Structure(
            name="Assimilator",
            details="Allows probes to harvest vespene gas from geysers.",
            specie_id=1,
        ),
        Structure(
            name="Baneling Nest",
            details="Required for baneling production and researches baneling upgrades.",
            specie_id=2,
        ),
        Structure(
            name="Barracks",
            details=" Produces terran infantry units.",
            specie_id=3,
        ),
    ]
    session.add_all(structures)
