from pathlib import Path

from isqlite import Database, Schema, Table, columns
from pandas import DataFrame, read_csv
from sqliteparser.ast import OnDelete

package_root = Path(__file__).parent
input_data_path = package_root / 'sample_sequencing_data.csv'
database_path = package_root / 'sequences.db'

SCHEMA = Schema([
    Table(
        'Sequence',
        [
            columns.primary_key('id'),
            columns.text('name', required=True),
        ]
    ),
    Table(
        'SequenceType',
        [
            columns.primary_key('id'),
            columns.foreign_key('sequence_id', 'Sequence', on_delete=OnDelete.CASCADE),
            columns.text('name', required=True),
        ]
    ),
    Table(
        'SequenceTypeLocation',
        [
            columns.primary_key('id'),
            columns.foreign_key('sequence_type_id', 'SequenceType', on_delete=OnDelete.CASCADE),
            columns.integer('location', required=True),
        ]
    )])


def setup_schema():
    with Database(database_path, transaction=False) as db:
        db.migrate(SCHEMA)


def populate_database():
    df = read_csv(input_data_path)
    df.drop('id', axis=1, inplace=True)
    df.drop_duplicates(inplace=True)
    with Database(database_path) as db:
        for seq_name in df.sequence_id.unique():
            seq_id = db.insert('Sequence', dict(name=seq_name))
            seq_df = df[df.sequence_id == seq_name]
            for seq_type in seq_df.type.unique():
                seq_type_df = seq_df[seq_df.type == seq_type]
                seq_type_id = db.insert('SequenceType', dict(sequence_id=seq_id, name=seq_type))
                locs = seq_type_df.location
                db.insert_many('SequenceTypeLocation',
                               [dict(sequence_type_id=seq_type_id, location=idx)
                                for idx in range(locs.min(), locs.max() + 1)])


if __name__ == '__main__':
    setup_schema()
    assert database_path.exists()
    try:
        populate_database()

        with Database(database_path) as db:
            match_id_col_name = 'match_seq_type_id'
            sequence_overlaps = db.sql(
                f'SELECT Sequence.name || " " || SequenceType.name AS sequence_type, '
                f'B.sequence_type_id AS {match_id_col_name}, A.location '
                'FROM SequenceTypeLocation A INNER JOIN SequenceTypeLocation B '
                'ON A.location = B.location '
                'INNER JOIN SequenceType ON A.sequence_type_id = SequenceType.id '
                'INNER JOIN Sequence ON SequenceType.sequence_id = Sequence.id '
                'WHERE A.sequence_type_id != B.sequence_type_id '
                'ORDER BY sequence_type, match_seq_type_id, A.location')
            sequence_to_type = db.sql(
                'SELECT Sequence.name || " " || SequenceType.name AS sequence_type, SequenceType.id '
                'FROM SequenceType INNER JOIN Sequence ON SequenceType.sequence_id = Sequence.id')
        sequence_to_type = DataFrame(sequence_to_type)
        sequence_overlaps = DataFrame(sequence_overlaps)
        merged = sequence_overlaps.merge(sequence_to_type, left_on=match_id_col_name, right_on='id')
        merged.drop([match_id_col_name, 'id'], axis=1, inplace=True)

        matched_seq_types = merged.drop('location', axis=1).drop_duplicates(['sequence_type_x', 'sequence_type_y'])
        print('Overlaps occur between:')
        for i in range(len(matched_seq_types)):
            seq_type_x, seq_type_y = matched_seq_types.iloc[i]
            matches = merged[merged.sequence_type_x == seq_type_x][merged.sequence_type_y == seq_type_y]
            loc_min, loc_max = matches.location.min(), matches.location.max()
            print(f'{seq_type_x} and {seq_type_y} at locations {loc_min} to {loc_max}')
    finally:
        database_path.unlink()
