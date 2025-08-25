import { View, StyleSheet } from "react-native";
import RNPickerSelect from "react-native-picker-select";

export default DropdownMenu = ({ tagsList, selectedTag, setSelectedTag }) => {
  const TAGS = tagsList.map((tag) => {
    return { label: tag.name, value: tag.name };
  });

  const getTagUuid = (value) => {
    const tagUuid = tagsList.filter((tag) => {
      return tag.name === value;
    });
    return tagUuid[0]?.uuid;
  };

  return (
    <View style={styles.container}>
      <RNPickerSelect
        items={TAGS}
        onValueChange={(value) =>
          setSelectedTag({ uuid: getTagUuid(value), name: value })
        }
        placeholder={{ label: "Select a category", value: null }}
        value={selectedTag}
        style={pickerSelectStyles}
      />
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    borderColor: "lightgray",
    borderWidth: 1,
    borderRadius: 5,
    backgroundColor: "#f0f5fd",
    marginBottom: 10,
  },
});

const pickerSelectStyles = StyleSheet.create({
  inputIOSContainer: { pointerEvents: "none" },
  inputIOS: {
    height: 50,
    fontSize: 16,
    paddingVertical: 12,
    paddingHorizontal: 10,
    borderWidth: 1,
    borderColor: "lightgray",
    borderRadius: 4,
    color: "black",
    backgroundColor: "#f0f5fd",
    paddingRight: 30,
  },
  inputAndroid: {
    height: 50,
    fontSize: 16,
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderWidth: 1,
    borderColor: "lightgray",
    borderRadius: 4,
    color: "black",
    backgroundColor: "#f0f5fd",
    paddingRight: 30,
  },
});
