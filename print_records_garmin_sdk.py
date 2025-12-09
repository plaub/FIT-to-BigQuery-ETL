from garmin_fit_sdk import Decoder, Stream

# Load the FIT file
stream = Stream.from_file("./test/Frankfurt_Marathon.fit")
decoder = Decoder(stream)

# Decode the FIT file
messages, errors = decoder.read()

# Print any errors encountered during decoding
if errors:
    print("Errors encountered during decoding:")
    for error in errors:
        print(f" * {error}")
    print("---")

# Iterate over all messages of type "file_id"
# (other types include "record", "session", "lap", "device_info", "event", etc)
if 'file_id_mesgs' in messages:
    print(f"Found {len(messages['file_id_mesgs'])} file_id message(s)")
    for file_id in messages['file_id_mesgs']:
        print("\nFile ID Message:")
        for field_name, field_value in file_id.items():
            print(f" * {field_name}: {field_value}")
        print("---")

# Also print some record messages to compare
if 'record_mesgs' in messages:
    print(f"\nFound {len(messages['record_mesgs'])} record message(s)")
    print("Showing first 5 records:")
    for i, record in enumerate(messages['record_mesgs'][:5]):
        print(f"\nRecord {i+1}:")
        for field_name, field_value in record.items():
            print(f" * {field_name}: {field_value}")
        print("---")

# Print all available message types
print("\nAvailable message types in this FIT file:")
for msg_type in messages.keys():
    print(f" * {msg_type}: {len(messages[msg_type])} message(s)")
