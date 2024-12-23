import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.htrace.fasterxml.jackson.annotation.JsonCreator;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.htrace.fasterxml.jackson.annotation.JsonProperty;

import java.lang.reflect.Field;
/*
{"date":"2024-09-13T07:27:05Z","game":"gdc","mode":"Rage_Ladder","round":0,"type":"riverRacePvP",
"winner":0,
"players":[{"utag":"#C82LPR8J","ctag":"#QQ9QGJYJ","trophies":9000,"ctrophies":1100,"exp":67,"league":6,
"bestleague":10,
"deck":"0a18323d4a4c616b",
"evo":"0a18",
"tower":"",
"strength":15.375,
"crown":2,
"elixir":1.56,
"touch":1,
"score":200},
{"utag":"#9UC2GUJVP","ctag":"#QQJCR9CP","trophies":7160,"ctrophies":1012,"exp":46,"league":1,"bestleague":4,"deck":"05070c14171f445e","evo":"","tower":"","strength":13,"crown":0,"elixir":6.14,"touch":1,"score":100}],"warclan":{"day":3,"hour_seg":3,"period":"112-1","training":[false,false]}}
*/

enum Game
{
	gdc,
	pathOfLegend
}

@JsonIgnoreProperties(ignoreUnknown=true)
class Player implements Serializable, Writable{
	
		@JsonProperty(value="utag", required=true)
		public String utag;
		@JsonProperty(value="ctag", required=true)
		public String ctag;
		@JsonProperty(value="trophies", required=true)
		public int trophies;
		@JsonProperty(value="ctrophies", required=true)
		public int ctrophies;
		@JsonProperty(value="exp", required=true)
		public int exp;
		@JsonProperty(value="league", required=true)
		public int league;
		@JsonProperty(value="bestleague", required=true)
		public int bestleague;
		@JsonProperty(value="deck", required=true)
		public String deck;
		@JsonProperty(value="evo", required=true)
		public String evo;
		@JsonProperty("tower")
		public String tower;
		@JsonProperty(value="strength", required=true)
		public float strength;
		@JsonProperty(value="elixir", required=true)
		public float elixir;
		@JsonProperty("touch")
		public int touch;//?
		@JsonProperty(value="score", required=true)
		public int score;
	
		@Override
		public void write(DataOutput out) throws IOException {
			// out.writeUTF(utag);
			out.writeUTF(ctag);
			out.writeInt(trophies);
			out.writeInt(ctrophies);
			out.writeInt(exp);
			out.writeInt(league);
			out.writeInt(bestleague);
	
			out.writeLong(Long.parseUnsignedLong(deck, 16));
			out.writeInt(Integer.parseUnsignedInt(evo, 16));
			out.writeChar(Integer.parseUnsignedInt(tower, 16));
			// out.writeUTF(deck);
			// out.writeUTF(evo);
			// out.writeUTF(tower);
	
			out.writeFloat(strength);
			out.writeFloat(elixir);
			out.writeBoolean(touch == 1);
			out.writeInt(score);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			// utag = in.readUTF();
			ctag = in.readUTF();
			trophies = in.readInt();
			ctrophies = in.readInt();
			exp = in.readInt();
			league = in.readInt();
			bestleague = in.readInt();
	
			deck = String.format("%016x", in.readLong());// Long.toHexString(in.readLong());
			evo = Integer.toHexString(in.readInt());//String.format("%04x", in.readInt());//
			tower = Integer.toHexString(in.readChar());
			// deck = in.readUTF();
			// evo = in.readUTF();
			// tower = in.readUTF();
	
			strength = in.readFloat();
			elixir = in.readFloat();
			touch = in.readBoolean()? 1 : 0;
			score = in.readInt();
		}
	
		public boolean isAnyEmptyOrNull()
		{
			return StringUtils.isEmpty(utag) || StringUtils.isEmpty(ctag) || StringUtils.isEmpty(deck)  || StringUtils.isEmpty(evo)  || StringUtils.isEmpty(tower);
		}
	}
	
	// class WarClan implements Serializable{
	// 	@JsonProperty("day")
	// 	public int day=0;
	// 	@JsonProperty("hourd_seg")
	// 	public int hour_seg=0;
	// 	@JsonProperty("period")
	// 	public String period;
	// 	@JsonProperty("training")
	// 	ArrayList<Boolean> training;	
	// }
	
	@JsonIgnoreProperties(ignoreUnknown=true)
	class Battle implements Serializable, Writable {
	
		@JsonProperty(value="date", required=true)	
		public String date;
		@JsonProperty(value="game", required=true)
		public String game;
		@JsonProperty(value="mode", required=true)
		public String mode;
		@JsonProperty(value="round", required=true)
		public int round;
		@JsonProperty(value="type", required=true)
		public String type;
		@JsonProperty(value="winner", required=true)
		public int winner;
		@JsonProperty(value="players", required=true)
		ArrayList<Player> players;
		// @JsonProperty("warclan")
		// WarClan warclan;
	
		@Override
		public void write(DataOutput out) throws IOException {
			//out.writeUTF(date);
			out.writeBoolean(game == "gdc");//out.writeChars(game);
			out.writeInt(mode.hashCode());//opt more?
			out.writeChar(round);
			out.writeInt(type.hashCode());
			out.writeBoolean(winner	== 1);
			out.writeChar(players.size());//player array length
			for (Player p : players) {
				p.write(out);
			}
		}
	
		@Override
		public void readFields(DataInput in) throws IOException {
			//date = in.readUTF();
			game = Game.values()[in.readBoolean()? 1 : 0].name();
			mode = String.valueOf(in.readInt());
			round = in.readChar();
			type = String.valueOf(in.readInt());
			winner = in.readBoolean()? 1 : 0;
			int nb_players = in.readChar();
			players = new ArrayList<Player>();
			for (int i = 0; i < nb_players; i++) {//-1?
				Player p = new Player();
			p.readFields(in);
			players.add(p);
		}
	}

	public String toString() {
		StringBuilder tmp = new StringBuilder();
		tmp.append(date);
		//...
		return tmp.toString();
	}

	public boolean isAnyEmptyOrNull()
	{
		// for (Field f : this.getClass().getFields()) {
		// 	try {
		// 		if(f.get(this) == null) return true;
		// 		if(f.getType().isAssignableFrom(String.class) && f.get(this) == "") return true;				
		// 	} catch (IllegalAccessException e) {
		// 		continue;
		// 	}
		// }

		if(StringUtils.isEmpty(date)|| StringUtils.isEmpty(game) || StringUtils.isEmpty(mode) || StringUtils.isEmpty(type) || players == null || players.isEmpty()) return true;

		for (Player player : players) {
			if(player.isAnyEmptyOrNull()) return true;
		}

		return false;
	}
}